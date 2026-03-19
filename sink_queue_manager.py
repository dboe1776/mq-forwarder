# src/mq_forwarder/sink_queue_manager.py

import asyncio
import structlog
from collections import defaultdict
from typing import Dict, List
from config_loader import AppConfig, SinkConfig
from models import DataPoint   # assuming you have to_data_point() on Message

logger = structlog.get_logger(__name__)

class SinkQueueManager:
    """
    Manages one asyncio.Queue + one flush consumer task per sink.
    Handles putting enriched DataPoints into the right queues.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.queues: Dict[str, asyncio.Queue[DataPoint]] = {}
        self.flush_tasks: Dict[str, asyncio.Task] = {}
        self._started = False

    async def start(self):
        """Create queues and start consumer tasks for all configured sinks."""
        if self._started:
            return

        for sink_name, sink_cfg in self.config.sinks.items():
            queue = asyncio.Queue(maxsize=10000)  # bounded to prevent runaway memory
            self.queues[sink_name] = queue

            task = asyncio.create_task(
                self._flush_loop(sink_name, sink_cfg, queue),
                name=f"sink-flush-{sink_name}"
            )
            self.flush_tasks[sink_name] = task

        self._started = True
        logger.info("sink_queues_started", sink_count=len(self.queues))

    async def stop(self):
        """Graceful shutdown: stop consumers and wait for tasks."""
        if not self._started:
            return

        for task in self.flush_tasks.values():
            task.cancel()

        await asyncio.gather(*self.flush_tasks.values(), return_exceptions=True)
        logger.info("sink_queues_stopped")

    async def _flush_loop(self, sink_name: str, sink_cfg: SinkConfig, queue: asyncio.Queue):
        batch_size = sink_cfg.batch_size
        flush_interval = sink_cfg.flush_interval_sec

        batch: List[DataPoint] = []

        try:
            while True:
                try:
                    # Wait for first item or timeout
                    item = await asyncio.wait_for(queue.get(), timeout=flush_interval)
                    batch.append(item)

                    # Drain remaining items up to batch_size - 1
                    while len(batch) < batch_size and not queue.empty():
                        try:
                            item = queue.get_nowait()
                            batch.append(item)
                        except asyncio.QueueEmpty:
                            break

                    if len(batch) >= batch_size:
                        await self._flush_batch(sink_name, batch)
                        batch.clear()

                except asyncio.TimeoutError:
                    if batch:
                        await self._flush_batch(sink_name, batch)
                        batch.clear()

                except asyncio.CancelledError:
                    # On shutdown: flush remaining
                    if batch:
                        await self._flush_batch(sink_name, batch)
                    raise

        except Exception:
            logger.exception("sink_flush_loop_crashed", sink_name=sink_name)
            # Future: restart logic?

    async def _flush_batch(self, sink_name: str, batch: List[DataPoint]):
        # Placeholder — replace with real sink write later
        logger.info(
            "sink_batch_flush",
            sink_name=sink_name,
            batch_size=len(batch),
            first_measurement=batch[0].measurement if batch else None,
            last_measurement=batch[-1].measurement if batch else None
        )
        # await real_sink_write(sink_name, batch)  # next step

    async def put_to_sinks(self, point: DataPoint, target_sink_names: List[str]):
        """Put the same DataPoint copy into each requested sink's queue."""
        for sink_name in target_sink_names:
            if sink_name not in self.queues:
                logger.warning("unknown_sink_dropping_point", sink_name=sink_name)
                continue
            try:
                await self.queues[sink_name].put(point)
            except asyncio.QueueFull:
                logger.warning("sink_queue_full_dropping", sink_name=sink_name)