import time
import random
from loguru import logger

from .util import acquire_label
from .schemas import TaskSchema
from .task_runner import get_runner
from .label_handler import LabelHandler


runner = get_runner()


@runner.add_task_function()
def sample_task_1(lh: LabelHandler, task: TaskSchema) -> str:
    """
    Example task function.
    :param lh: LabelHandler instance for managing labels.
    :param task: TaskSchema instance containing task details.
    :return: A string indicating the task completion.
    """
    logger.info("Executing sample_task_1 with task_id: {}", task.task_id)
    acquire_label(lh, task.label, task.task_id, lh.runner_uuid)

    time.sleep(
        max(0.1, random.normalvariate(1.0, 0.5))
    )  # Simulate task processing time
    logger.debug("Completed sample_task_1 with task_id: {}", task.task_id)
    return f"Task {task.task_id} completed by sample_task_1"


@runner.add_task_function()
def sample_task_2(lh: LabelHandler, task: TaskSchema) -> str:
    """
    Example task function.
    :param lh: LabelHandler instance for managing labels.
    :param task: TaskSchema instance containing task details.
    :return: A string indicating the task completion.
    """
    logger.debug("Executing sample_task_2 with task_id: {}", task.task_id)
    acquire_label(lh, task.label, task.task_id, lh.runner_uuid)

    time.sleep(
        max(0.5, random.normalvariate(5.0, 1.0))
    )  # Simulate somewhat longer task processing time
    logger.debug("Completed sample_task_2 with task_id: {}", task.task_id)
    return f"Task {task.task_id} completed by sample_task_2"
