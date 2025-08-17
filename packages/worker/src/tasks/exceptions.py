class TaskError(Exception):
    """
    Base class for all task-related exceptions.
    """

    pass


class UnknownTaskError(TaskError):
    """
    Exception raised when a task is not recognized or does not exist.
    """

    pass


class TaskFailedError(TaskError):
    """
    Exception raised when a task fails to execute successfully.
    """

    pass
