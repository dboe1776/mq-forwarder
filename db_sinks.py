import httpx
import gzip
import aiosqlite
import structlog
import asyncio
import json
from typing import List, Dict, Any
from enum import StrEnum

from models import DataPoint, to_line_protocol  # assuming to_line_protocol is exported here
from config_loader import SinkConfig

logger = structlog.get_logger(__name__)

class TryAgainNowError(Exception):
    """Transient error - retry immediately (e.g. connection refused)."""
    pass

class TryAgainLaterError(Exception):
    """Recoverable error - retry after delay (e.g. rate limit, server 5xx)."""
    pass

class BadDataError(Exception):
    """Permanent data problem - discard or dead-letter (e.g. 400/422)."""
    pass

class PostStatusCodes(StrEnum):
    SUCCESS    = "success"
    FAIL       = "fail"        # permanent failure – move to failed folder
    TRY_LATER  = "try_later"   # retry after backoff

class SinkPosterManager:
    """
    Manages one persistent poster instance per sink config.
    Allows keeping httpx clients alive for connection pooling.
    """

    def __init__(self, sink_configs: Dict[str,SinkConfig]):
        logger.debug("initializing_sink_poster_manager", sink_count=len(sink_configs))
        self.posters: Dict[str, BaseDataPoster] = {}

        available_posters = {c.handles: c for c in BaseDataPoster.__subclasses__()}

        self.posters = {name:available_posters.get(s.type,BaseDataPoster)(s) for name,s in sink_configs.items()}

    async def post_data(self,sink_name: str, bundle: List[DataPoint]) -> PostStatusCodes:
        if not bundle:
            return PostStatusCodes.SUCCESS

        logger.debug(
            "posting_bundle",
            sink_name=sink_name,
            bundle_size=len(bundle),
            first_time=bundle[0].time_ns if bundle else None,
            last_time=bundle[-1].time_ns if bundle else None
        )

        poster = self.posters.get(sink_name)

        if not poster:
            logger.error("no_poster_registered", sink_name=sink_name)
            return PostStatusCodes.FAIL

        try:
            await poster.post_data(bundle)
            logger.info(
                "sink_post_success",
                sink_name=sink_name,
                points=len(bundle)
            )
            return PostStatusCodes.SUCCESS

        except TryAgainNowError as e:
            logger.warning(
                "transient_error_try_now",
                sink_name=sink_name,                
                error=str(e),
                bundle_size=len(bundle)
            )
            return PostStatusCodes.TRY_LATER  # caller can retry immediately

        except TryAgainLaterError as e:
            logger.warning(
                "retry_later",
                sink_name=sink_name,                
                error=str(e),
                bundle_size=len(bundle)
            )
            return PostStatusCodes.TRY_LATER

        except BadDataError as e:
            logger.error(
                "permanent_bad_data",
                sink_name=sink_name,
                error=str(e),
                bundle_size=len(bundle),
                first_lp=to_line_protocol(bundle[0])[:200] if bundle else None
            )
            return PostStatusCodes.FAIL

        except Exception as e:
            logger.exception(
                "unexpected_poster_failure",
                sink_name=sink_name,      
                bundle_size=len(bundle)
            )
            return PostStatusCodes.TRY_LATER  # conservative fallback

    async def close(self):
        for poster in self.posters.values():
            if hasattr(poster, "close") and callable(poster.close):
                await poster.close()

class BaseDataPoster:
    handles: str = "default"

    def __init__(self, sink_config: SinkConfig):
        self.config = sink_config

    async def post_data(self, bundle: List[DataPoint]):
        raise NotImplementedError(
            f"Poster for {self.handles!r} not implemented "
            f"(called for sink type {self.config.type})"
        )

    async def close(self):
        pass

class InfluxDataPoster(BaseDataPoster):
    handles: str = "influxdb"

    def __init__(self, sink_config: SinkConfig):
        super().__init__(sink_config)
        extra = sink_config.extra

        self.url = extra.get("url", "").rstrip("/")
        if not self.url:
            raise ValueError("Influx sink missing 'url'")

        self.version = extra.get("version", "v2").lower()  # "v1" or "v2"

        self.token = extra.get("token")
        self.username = extra.get("username")
        self.password = extra.get("password")
        self.org = extra.get("org")
        self.database = extra.get('database')
        self.precision = extra.get('precision')
        self.bucket = extra.get("bucket")
        self.precision = extra.get("precision", "ns")

        self.compress = extra.get("compress", True)
        self.max_retries = extra.get("max_retries", 3)

        # Persistent HTTP client for connection pooling
        self.client = httpx.AsyncClient(http2=False, timeout=30.0)

    async def post_data(self, bundle: List[DataPoint]):
        if self.version == "v2":
            await self._post_v2(bundle)
        else:
            await self._post_v1(bundle)

    async def _post_v2(self, bundle: List[DataPoint]):
        if not all([self.token, self.org, self.bucket]):
            raise ValueError("Influx v2 requires token, org, bucket")

        url = f"{self.url}/api/v2/write"
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "text/plain; charset=utf-8",
        }
        params = {"org": self.org, "bucket": self.bucket, "precision": self.precision}

        # One shot this, let's make use of list comprehensions. They are pythonic
        lp_lines = [to_line_protocol(p,ignore_errors=True) for p in bundle]
        lp_lines = [l for l in lp_lines if l]
        if not lp_lines:
            return

        body = "\n".join(lp_lines).encode("utf-8")
        if self.compress:
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"

        await self._http_post(url, params, body, headers)

    async def _post_v1(self, bundle: List[DataPoint]):
        if not all([self.username, self.password]):
            raise ValueError("Influx v1 requires username and password")

        url = f"{self.url}/write"
        params = {"db": self.database or "default", "precision": self.precision}
        auth = httpx.BasicAuth(self.username, self.password)
        headers = {"Content-Type": "text/plain; charset=utf-8"}

        lp_lines = [to_line_protocol(p,ignore_errors=True) for p in bundle]
        lp_lines = [l for l in lp_lines if l]

        if not lp_lines:
            return
        body = "\n".join(lp_lines).encode("utf-8")
        
        if self.compress:
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
    
        await self._http_post(url, params, body, headers, auth=auth)

    async def _http_post(self, url: str, params: dict, content: bytes, headers: dict, auth=None):
            for attempt in range(1, self.max_retries + 1):
                try:
                    r = await self.client.post(
                        url, params=params, content=content, headers=headers, auth=auth
                    )
                    if r.status_code in (200, 204):
                        return
                    if r.status_code in (400, 422):
                        raise BadDataError(f"Influx rejected: {r.text[:600]}")
                    if r.status_code in (429, 500, 502, 503, 504):
                        delay = 2 ** attempt
                        logger.warning("influx_retry", attempt=attempt, status=r.status_code, delay=delay)
                        await asyncio.sleep(delay)
                        continue
                    raise TryAgainLaterError(f"Status {r.status_code}")
                except httpx.RequestError as exc:
                    if attempt == self.max_retries:
                        raise TryAgainLaterError(f"Request failed: {exc}")
                    await asyncio.sleep(2 ** attempt)

            raise TryAgainLaterError("Retries exhausted")

    async def close(self):
        await self.client.aclose()

class QuestdbDataPoster(BaseDataPoster):
    handles: str = "questdb"

    def __init__(self, sink_config: SinkConfig):
        super().__init__(sink_config)
        extra = sink_config.extra

        self.url = extra.get("url", "").rstrip("/") + '/write'
        self.username = extra.get("username")
        self.password = extra.get("password")
        self.compress = extra.get("compress", True)
        self.max_retries = extra.get("max_retries", 3)
        self.precision = extra.get('precision')

        self.client = httpx.AsyncClient(http2=False, timeout=30.0)

    async def post_data(self, bundle: List[DataPoint]):

        # params = {
        #     "timestamp": "time_ns",  # column name for timestamp
        # }
        params = None

        headers = {"Content-Type": "text/plain; charset=utf-8",}

        lp_lines = [to_line_protocol(p,ignore_errors=True) for p in bundle]
        lp_lines = [l for l in lp_lines if l]

        body = "\n".join(lp_lines).encode("utf-8")
        if self.compress:
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"

        auth = None
        if self.username and self.password:
            auth = httpx.BasicAuth(self.username, self.password)

        for attempt in range(1, self.max_retries + 1):
            try:
                r = await self.client.post(
                    self.url,
                    params=params,
                    content=body,
                    headers=headers,
                    auth=auth,
                )

                if r.status_code in (200, 204):
                    logger.info("questdb_write_success", points=len(bundle))
                    return

                if r.status_code in (400, 422):
                    raise BadDataError(f"QuestDB rejected: {r.text[:500]}")

                if r.status_code >= 500 or r.status_code == 429:
                    delay = 2 ** attempt
                    logger.warning("questdb_retry", attempt=attempt, status=r.status_code)
                    await asyncio.sleep(delay)
                    continue

                raise TryAgainLaterError(f"Unexpected status {r.status_code}")

            except httpx.RequestError as exc:
                if attempt == self.max_retries:
                    raise TryAgainLaterError(f"QuestDB request failed: {exc}")
                await asyncio.sleep(2 ** attempt)

        raise TryAgainLaterError("QuestDB retries exhausted")

    async def close(self):
        await self.client.aclose()

class SqliteDataPoster(BaseDataPoster):
    handles: str = "sqlite"

    def __init__(self, sink_config: SinkConfig):
        super().__init__(sink_config)
        extra = sink_config.extra
        self.db_path = extra.get("path")
        if not self.db_path:
            raise ValueError("SQLite sink missing 'path'")

        self.table_name = extra.get("table_name", "measurements")
        self.batch_size = sink_config.batch_size

    async def post_data(self, bundle: List[DataPoint]):
        if not bundle:
            return

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")  # better concurrency

            # Create table if not exists
            await db.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    timestamp REAL NOT NULL,
                    measurement TEXT NOT NULL,
                    tags JSON,
                    fields JSON,
                    PRIMARY KEY (timestamp, measurement)
                )
            """)

            rows = []
            for p in bundle:
                tags_json = json.dumps(p.tags) if p.tags else None
                fields_json = json.dumps(p.fields) if p.fields else None
                rows.append((p.time, p.meas, tags_json, fields_json))

            await db.executemany(
                f"INSERT OR REPLACE INTO {self.table_name} "
                f"(timestamp, measurement, tags, fields) VALUES (?, ?, ?, ?)",
                rows
            )

            await db.commit()

        logger.info("sqlite_write_success", rows=len(bundle), path=self.db_path)

    async def close(self):
        pass  # no persistent connection needed