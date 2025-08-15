import redis
from uuid import uuid4
from loguru import logger
from types import TracebackType
from typing import ContextManager, Type, Optional, Iterator

from . import constants as const
from .settings import WorkerSettings
from .label_handler import LabelHandler


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

    def listen(self) -> Iterator[str]:
        """
        Listen for tasks on the task runner's stream.
        """
        logger.info("Task runner listening for tasks [{}]", self.__queue)
        while True:
            try:
                queue, task = self.__redis.blpop(
                    [self.__queue, const.COMMON_QUEUE], timeout=0
                )
                logger.info("Received task from queue [{}]", queue)
                if task:
                    yield task

            except redis.ConnectionError as e:
                logger.error("Redis connection error: {}", e)
                break
            except Exception as e:
                logger.error("Error while listening for tasks: {}", e)
                break
