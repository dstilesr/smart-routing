import time
import redis
from uuid import uuid4
from loguru import logger
from types import TracebackType
from pydantic import ValidationError
from functools import wraps, lru_cache
from typing import ContextManager, Type, Optional, Iterator, Callable

from . import exceptions as err
from .schemas import TaskSchema
from . import constants as const
from .settings import WorkerSettings
from .label_handler import LabelHandler


TASK_TYPE = Callable[[LabelHandler, TaskSchema], ...]


class TaskRunner(ContextManager):
    """
    Task runner to handle worker tasks.
    """

    def __init__(self, settings: WorkerSettings | None = None):
        """
        Initialize the task runner with settings.
        """
        self.__settings = settings or WorkerSettings()
        self.__redis = redis.Redis(
            host=self.__settings.redis_host,
            port=self.__settings.redis_port,
            decode_responses=True,
        )
        self.__uuid = str(uuid4())
        self.__label_handler = LabelHandler(
            runner_uuid=self.uuid,
            redis_client=self.__redis,
            max_labels=self.__settings.max_labels,
        )
        self.__queue = f"task-runners:{self.uuid}:jobs"
        self.__task_handlers: dict[str, TASK_TYPE] = {}

    @property
    def uuid(self) -> str:
        """
        Get the unique identifier for this task runner.
        """
        return self.__uuid

    @property
    def label_handler(self) -> LabelHandler:
        """
        Get the label handler for this task runner.
        """
        return self.__label_handler

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        self.register()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        """
        Exit runtime.
        """
        self.deregister()
        self.__redis.close()
        if exc_value:
            logger.error(
                "Task runner exited with exception [{}.{}] {}",
                exc_type.__module__,
                exc_type.__name__,
                exc_value,
            )

    def register(self):
        """
        Register task runner on redis.
        """
        self.update_availability(True)
        self.__redis.sadd(const.REGISTER_KEY, self.uuid)
        logger.info("Task runner registered [{}]", self.uuid)

    def deregister(self):
        """
        Deregister task runner from redis.
        """
        self.update_availability(False)
        self.__redis.srem(const.REGISTER_KEY, self.uuid)
        self.label_handler.clear_all()
        logger.info("Task runner deregistered [{}]", self.uuid)

    def update_availability(self, available: bool = True):
        """
        Mark the task runner as available.
        :param available: Whether the task runner is available or not.
        """
        if available:
            self.__redis.sadd(const.AVAILABLE_KEY, self.uuid)
        else:
            self.__redis.srem(const.AVAILABLE_KEY, self.uuid)

    def get_task_handler(self, task_type: str) -> TASK_TYPE:
        """
        Get the task handler for a specific task type.
        :param task_type: The type of the task to get the handler for.
        :return: The task handler function.
        """
        if task_type not in self.__task_handlers:
            raise err.UnknownTaskError(
                f"Task type '{task_type}' is not registered."
            )
        return self.__task_handlers[task_type]

    def listen(self) -> Iterator[str]:
        """
        Listen for tasks on the task runner's stream.
        """
        logger.info("Task runner listening for tasks [{}]", self.__queue)
        while True:
            task = None
            try:
                queue, task_raw = self.__redis.blpop(
                    [self.__queue, const.COMMON_QUEUE], timeout=0
                )
                logger.info("Received task from queue [{}]", queue)
                if not task_raw:
                    continue

                task = TaskSchema.model_validate_json(task_raw)
                handler = self.get_task_handler(task.task_type)
                result = handler(self.label_handler, task)
                yield result

            except err.TaskError as e:
                bind = {}
                if task is not None:
                    bind = {"task_id": task.task_id}

                logger.bind(**bind).error(
                    "Task [{}] failed with error: {}",
                    task.task_id,
                    e,
                )
                yield "Task failed"

            except ValidationError as e:
                logger.error("Invalid Task received! {}", e)
                continue

            except redis.ConnectionError as e:
                logger.error("Redis connection error: {}", e)
                break

            except Exception as e:
                logger.error(
                    "Error while listening for tasks: [{}.{}]{}",
                    type(e).__module__,
                    type(e).__name__,
                    e,
                )
                break

    def add_task_function(
        self, task_type: str | None = None
    ) -> Callable[[TASK_TYPE], TASK_TYPE]:
        """
        Register a function that will handle tasks.
        :param task_type: Optional type of the task to be handled. If not
            provided, the function name will be used.
        :return:
        """

        def decorator(func: TASK_TYPE) -> TASK_TYPE:
            """
            Decorator to register a task handler function.
            """
            nonlocal task_type
            if not task_type:
                task_type = func.__name__

            @wraps(func)
            def wrapper(lh: LabelHandler, task: TaskSchema):
                result = None
                start = time.perf_counter()
                try:
                    self.update_availability(False)
                    result = func(lh, task)
                except Exception as e:
                    logger.error(
                        "Error while executing task [{}]: {}",
                        task_type,
                        e,
                    )
                    raise err.TaskFailedError(
                        f"Task [{task.task_id}] failed with error: {e}"
                    )
                finally:
                    self.update_availability(True)

                if task.return_result:
                    self.__redis.publish(
                        f"task-runners:results:{task.task_id}",
                        result,
                    )

                end = time.perf_counter()
                logger.info(
                    "Task [{}] from worker [{}] completed in {:.6f} seconds",
                    task.task_id,
                    self.uuid,
                    end - start,
                )
                return result

            self.__task_handlers[task_type] = wrapper
            logger.info(
                "Registered Task Handler [{}] for type [{}]",
                func.__name__,
                task_type,
            )
            return wrapper

        return decorator


@lru_cache(maxsize=1)
def get_runner() -> TaskRunner:
    """
    Get the current task runner instance.
    :return: The current task runner instance.
    """
    return TaskRunner()
