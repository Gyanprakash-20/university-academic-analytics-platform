import sys
from loguru import logger
from config.settings import settings

logger.remove()

logger.add(
    sys.stderr,
    level=settings.log_level,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} - {message}",
    colorize=True,
)

logger.add(
    "logs/university_analytics_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="30 days",
    compression="zip",
    level="DEBUG",
    enqueue=True,
)

__all__ = ["logger"]