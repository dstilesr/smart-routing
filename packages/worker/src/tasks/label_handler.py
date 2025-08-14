import redis
from loguru import logger
from datetime import datetime, UTC
from collections import OrderedDict

from . import constants as const


class LabelHandler:
    """
    Class for handling the worker's labels. This works as an LRU cache with
    a maximum number of labels that can be stored.
    """

    def __init__(
        self, runner_uuid: str, redis_client: redis.Redis, max_labels: int = 2
    ):
        """
        :param runner_uuid: Unique identifier for the task runner.
        :param redis_client: Redis client instance.
        :param max_labels: Maximum number of labels to store.
        """
        self.__redis = redis_client
        self.__loaded_labels: OrderedDict[str, datetime] = OrderedDict()
        self.__max_labels = max_labels
        self.__runner_uuid = runner_uuid

    @property
    def runner_uuid(self) -> str:
        """
        Get the unique identifier for the task runner that owns this label
        handler.
        """
        return self.__runner_uuid

    @property
    def redis(self) -> redis.Redis:
        """
        Get the Redis client.
        """
        return self.__redis

    @property
    def max_labels(self) -> int:
        """
        Get the maximum number of labels allowed.
        """
        return self.__max_labels

    def add_label(self, label: str):
        """
        Register a label for the worker.
        :param label:
        :return:
        """
        if label in self.__loaded_labels:
            self.__loaded_labels.pop(label)
            self.__loaded_labels[label] = datetime.now(UTC)
            logger.debug("Label refreshed [{}]", label)

        elif len(self.__loaded_labels) >= self.max_labels:
            old, loaded_at = self.__loaded_labels.popitem(last=False)
            self.__deregister_label(old)
            logger.info(
                "Removed oldest label [{}] loaded at [{:%Y-%m-%dT%H:%M:%SZ}]",
                old,
                loaded_at,
            )
            self.__loaded_labels[label] = datetime.now(UTC)
            logger.debug("Label added [{}]", label)

        else:
            self.__loaded_labels[label] = datetime.now(UTC)
            logger.info("Label added [{}]", label)

        self.redis.sadd(
            const.LABEL_KEY_FMT.format(label=label), self.runner_uuid
        )

    def remove_label(self, label: str) -> datetime | None:
        """
        Remove a label from the worker.
        :param label: Label to remove.
        :return: The datetime the label was added, or None if not found.
        """
        self.__deregister_label(label)
        return self.__loaded_labels.pop(label, None)

    def has_label(self, label: str) -> bool:
        """
        Check if the worker has a specific label.
        :param label: Label to check.
        :return: True if the label exists, False otherwise.
        """
        return label in self.__loaded_labels

    def __deregister_label(self, label: str):
        """
        Deregister a label from the worker in Redis.
        :param label: Label to deregister.
        """
        self.redis.srem(
            const.LABEL_KEY_FMT.format(label=label), self.runner_uuid
        )
        logger.debug("Label deregistered [{}]", label)

    def clear_all(self):
        """
        Remove all labels from the worker.
        """
        for label in list(self.__loaded_labels.keys()):
            self.remove_label(label)

    def __len__(self) -> int:
        """
        Get the number of labels currently loaded.
        :return: Number of labels.
        """
        return len(self.__loaded_labels)
