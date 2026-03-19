# main.py

import asyncio
import argparse
import signal
import sys

# Windows asyncio fix — must be very early
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import structlog

from config_loader import load_config, AppConfig
from listen import run_all_listeners
from persistence import PersistentSinkDispatcher

logger = structlog.get_logger(__name__)

async def main(config_path: str = "config.toml"):

    config: AppConfig = load_config(config_path)
    logger.info("config_loaded", path=config_path)

    # Start persistence layer (creates poster_manager internally)
    dispatcher = PersistentSinkDispatcher(config)
    await dispatcher.start()

    try:
        await run_all_listeners(config,dispatcher)
    except KeyboardInterrupt:
        logger.info("shutdown_requested_keyboard_interrupt")
    except Exception as e:
        logger.exception("runtime_error", error=str(e))
        raise
    finally:
        logger.info("shutdown_started")
        if dispatcher is not None:
            await dispatcher.stop()   # this will also close the poster_manager
        logger.info("shutdown_complete")


def setup_logging(log_level: str = "INFO"):
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level.upper()),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT → DB Forwarder")
    parser.add_argument("--config", default="config.toml", help="Path to config file")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()

    setup_logging(args.log_level)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown(sig=None, frame=None):
        logger.info("shutdown_signal_received", signal=sig)
        loop.call_soon_threadsafe(loop.stop)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(main(args.config))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("startup_failure", error=str(e))
        sys.exit(1)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logger.info("process_exit")