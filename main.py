import structlog
import sys
import asyncio
import listen
from config_loader import load_config


def setup_logging(level: str = "INFO"):
    shared_processors = [
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.dict_tracebacks,  # nice for exceptions
    ]

    if level.upper() == "DEBUG":
        # Pretty console output during dev
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty()),
        ]
        renderer = structlog.dev.ConsoleRenderer()
    else:
        # JSON for production (easy for log shippers like Loki, Datadog, etc.)
        processors = shared_processors + [
            structlog.processors.JSONRenderer(),
        ]
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog.processors, level.upper())
        ),
        logger_factory=structlog.WriteLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )




async def main():
    config = load_config("config.toml")
    async for msg in listen.mqtt_listener_from_config(config):
        # For now just observe
        print(f"Processed: {msg.id} | {msg.measurement}")


if __name__ == '__main__':
    setup_logging("DEBUG")  # or "INFO" in prod
    asyncio.run(main())