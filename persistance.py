# src/mq_forwarder/persistence.py

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
import structlog

from config_loader import AppConfig, SinkConfig
from models import DataPoint, to_line_protocol

logger = structlog.get_logger(__name__)

class PersistentSinkDispatcher:
    """
    Per-sink persistent append + background flush.
    Immediate disk write on each point for durability.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.base_dir: Path = Path(config.data_dir) / "sinks"
        self.pending_dirs: Dict[str, Path] = {}
        self.current_files: Dict[str, Path] = {}  # sink_name -> current.jsonl Path
        self.flush_tasks: Dict[str, asyncio.Task] = {}
        self._started = False

        # Configurable thresholds
        self.max_file_size_bytes = 100_000   # 100 KB
        self.flush_interval_sec = 10         # wake up even if no new data
        self.batch_size_lines = 500          # process up to this many lines per flush

    async def start(self):
        if self._started:
            return

        self.base_dir.mkdir(parents=True, exist_ok=True)

        for sink_name, sink_cfg in self.config.sinks.items():
            pending_dir = self.base_dir / sink_name / "pending"
            pending_dir.mkdir(parents=True, exist_ok=True)
            self.pending_dirs[sink_name] = pending_dir

            current_file = pending_dir / "current.jsonl"
            self.current_files[sink_name] = current_file

            # Start flush consumer
            task = asyncio.create_task(
                self._flush_loop(sink_name, sink_cfg),
                name=f"flush-{sink_name}"
            )
            self.flush_tasks[sink_name] = task

        self._started = True
        logger.info("persistent_dispatcher_started", sink_count=len(self.config.sinks))

    async def stop(self):
        if not self._started:
            return

        for task in self.flush_tasks.values():
            task.cancel()

        await asyncio.gather(*self.flush_tasks.values(), return_exceptions=True)
        logger.info("persistent_dispatcher_stopped")

    async def dispatch_point(self, point: DataPoint, sink_names: List[str]):
        """Append point to each target sink's current file."""
        serialized = point.to_json() + "\n"

        for sink_name in sink_names:
            if sink_name not in self.current_files:
                logger.warning("unknown_sink_dropping", sink_name=sink_name)
                continue

            file_path = self.current_files[sink_name]
            try:
                async with aiofiles.open(file_path, mode="a", encoding="utf-8") as f:
                    await f.write(serialized)

                # Check if rotation needed
                stat = file_path.stat()
                if stat.st_size > self.max_file_size_bytes:
                    await self._rotate_file(sink_name)

            except Exception:
                logger.exception("append_failed", sink_name=sink_name, point_id=point.id)

    async def _rotate_file(self, sink_name: str):
        current = self.current_files[sink_name]
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        new_name = f"batch-{timestamp}.jsonl"
        new_path = current.parent / new_name

        await asyncio.to_thread(current.rename, new_path)  # rename is atomic on most FS
        logger.debug("file_rotated", sink_name=sink_name, old=current.name, new=new_name)

        # New empty current file
        current.touch()

    async def _flush_loop(self, sink_name: str, sink_cfg: SinkConfig):
        pending_dir = self.pending_dirs[sink_name]

        while True:
            try:
                # Find all completed batch files (exclude current.jsonl)
                files = sorted(
                    [f for f in pending_dir.iterdir() if f.name != "current.jsonl"],
                    key=lambda p: p.stat().st_mtime
                )

                if not files:
                    await asyncio.sleep(self.flush_interval_sec)
                    continue

                # Process oldest first
                for file_path in files[:5]:  # limit per loop to avoid overload
                    await self._process_file(sink_name, file_path, sink_cfg)

                await asyncio.sleep(self.flush_interval_sec)

            except asyncio.CancelledError:
                # On shutdown: try to flush any pending files
                for file_path in pending_dir.iterdir():
                    if file_path.name != "current.jsonl":
                        await self._process_file(sink_name, file_path, sink_cfg)
                raise
            except Exception:
                logger.exception("flush_loop_error", sink_name=sink_name)
                await asyncio.sleep(10)  # backoff

    async def _process_file(self, sink_name: str, file_path: Path, sink_cfg: SinkConfig):
        flushing_dir = file_path.parent.parent / "flushing"
        flushing_dir.mkdir(exist_ok=True)

        flushing_path = flushing_dir / file_path.name

        try:
            # Atomic move to flushing/
            await asyncio.to_thread(file_path.rename, flushing_path)

            lines = []
            async with aiofiles.open(flushing_path, mode="r", encoding="utf-8") as f:
                async for line in f:
                    try:
                        data = json.loads(line.strip())
                        point = DataPoint(**data)  # or reconstruct as needed
                        lines.append(point)
                    except Exception:
                        logger.warning("invalid_line_skipped", file=file_path.name)

            if not lines:
                await asyncio.to_thread(flushing_path.unlink)
                return

            # Try to flush to backend
            success = await self._flush_batch_to_backend(sink_name, sink_cfg, lines)

            if success:
                await asyncio.to_thread(flushing_path.unlink)
                logger.info("batch_flushed_success", sink=sink_name, count=len(lines))
            else:
                failed_dir = file_path.parent.parent / "failed"
                failed_dir.mkdir(exist_ok=True)
                failed_path = failed_dir / file_path.name
                await asyncio.to_thread(flushing_path.rename, failed_path)
                logger.warning("batch_flush_failed_moved", sink=sink_name, file=failed_path.name)

        except Exception:
            logger.exception("process_file_failed", sink=sink_name, file=file_path.name)
            # Move back to pending if possible, or leave in flushing

    async def _flush_batch_to_backend(
        self,
        sink_name: str,
        sink_cfg: SinkConfig,
        batch: List[DataPoint]
    ) -> bool:
        # Placeholder — replace with real implementation per sink type
        logger.info(
            "backend_flush_attempt",
            sink=sink_name,
            type=sink_cfg.type,
            batch_size=len(batch)
        )

        # For testing: pretend success after logging
        # Later: if sink_cfg.type == "influxdb": await influx_write(...)
        #       elif sink_cfg.type == "sqlite": await sqlite_write(...)

        await asyncio.sleep(0.1)  # simulate work
        return True  # change to False to test failure path