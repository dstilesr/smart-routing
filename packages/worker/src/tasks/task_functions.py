import time
from loguru import logger

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
    time.sleep(2)
    logger.info("Completed sample_task_1 with task_id: {}", task.task_id)
    return f"Task {task.task_id} completed by sample_task_1"
