import time
import random
from loguru import logger

from .label_handler import LabelHandler


# Suppose it takes on average 2 seconds to acquire a label, with a standard deviation of 0.25 seconds.
_MEAN: float = 2.0
_SD: float = 0.25


def acquire_label(
    lh: LabelHandler,
    label: str | None,
    task_id: str | None = None,
    worker_id: str | None = None,
) -> bool:
    """
    Function to 'acquire' a label. Simulates the process of loading some data.
    :param lh: LabelHandler instance for managing labels.
    :param label:
    :param task_id: Optional task ID for logging purposes.
    :param worker_id: Optional worker ID for logging purposes.
    :return: True if the label was already acquired, False if it was not.
    """
    if label is None or lh.has_label(label):
        return True

    logger.bind(task_id=task_id, worker_id=worker_id).warning(
        "LABEL MISS: <{}>", label
    )
    duration = random.normalvariate(_MEAN, _SD)
    duration = max(duration, 0.2)  # Ensure a minimum duration of 0.2 seconds
    time.sleep(duration)
    lh.add_label(label)
    return False
