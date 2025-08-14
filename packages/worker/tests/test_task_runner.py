import pytest

from tasks import constants as const
from tasks.task_runner import TaskRunner


@pytest.fixture(scope="session")
def redis_client():
    """
    Fixture to create a mock Redis client for testing.
    This uses a local Redis server running on the default port.
    """
    import redis

    return redis.Redis(host="localhost", port=6379, decode_responses=True)


@pytest.fixture(scope="function")
def runner(redis_client):
    """
    Fixture to create a TaskRunner instance with live Redis client.
    """
    from tasks.settings import WorkerSettings

    settings = WorkerSettings(redis_host="localhost", redis_port=6379, max_labels=2)
    runner = TaskRunner(settings=settings)
    yield runner
    runner.label_handler.clear_all()
    redis_client.flushdb()


@pytest.mark.live_redis
def test_runner_startup_shutdown(runner, redis_client):
    """
    Test the startup and shutdown of the TaskRunner.
    """
    with runner:
        assert redis_client.sismember(const.REGISTER_KEY, runner.uuid), \
            "Runner should be registered in Redis"

    assert not redis_client.sismember(const.REGISTER_KEY, runner.uuid), \
        "Runner should be unregistered after shutdown"


@pytest.mark.live_redis
def test_runner_label_register(runner, redis_client):
    """
    Test the label registration functionality of the TaskRunner.
    """
    with runner:
        assert redis_client.sismember(const.REGISTER_KEY, runner.uuid), \
            "Runner should be registered in Redis"

        label = "test-label"
        runner.label_handler.add_label(label)
        key = const.LABEL_KEY_FMT.format(label=label)

        assert redis_client.sismember(key, runner.uuid), \
            f"Runner UUID should be registered under label '{label}'"


@pytest.mark.live_redis
def test_runner_label_deregister(runner, redis_client):
    """
    Test the label deregistration functionality of the TaskRunner.
    """
    with runner:
        assert redis_client.sismember(const.REGISTER_KEY, runner.uuid), \
            "Runner should be registered in Redis"

        label = "test-label"
        runner.label_handler.add_label(label)
        key = const.LABEL_KEY_FMT.format(label=label)

    assert not redis_client.sismember(key, runner.uuid), \
        "Runner UUID should be deregistered from label on shutdown"
