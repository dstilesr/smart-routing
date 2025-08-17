import sys
import json
import requests
from loguru import logger
from functools import partial

from ._settings import LoggingSettings


def _send_to_collector(log_msg: str, host: str):
    """
    Send a log message to the log collector service.
    :param log_msg: Log message to send.
    :param host: Host URL of the log collector service.
    """
    requests.post(
        f"{host}/log",
        data=formatter(log_msg.record),
        headers={"Content-Type": "text/plain"},
    )


def formatter(record: dict) -> str:
    """
    JSON formatter for log records.
    :param record:
    :return:
    """
    log_rec = {
        "time": record["time"].isoformat(),
        "level": record["level"].name,
        "msg": record["message"],
        "name": record["name"],
        "module": record["module"],
        "function": record["function"],
        "line": record["line"],
        **record.get("extra", {}),
    }
    return json.dumps(log_rec) + "\n"


def setup_logs(settings: LoggingSettings | None = None):
    """
    Setup logging configuration for the worker.
    :param settings: LoggingSettings instance or None to use defaults.
    :return:
    """
    settings = settings or LoggingSettings()

    # Clear previous handlers
    logger.remove()
    logger.add(sys.stderr, level=settings.base_level)

    if settings.collector_enabled:
        logger.add(
            partial(_send_to_collector, host=settings.collector_host),
            level=settings.collector_level,
            serialize=False,
            enqueue=True,
        )
