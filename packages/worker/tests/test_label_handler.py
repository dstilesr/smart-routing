import pytest

from tasks.label_handler import LabelHandler
from tasks.constants import LABEL_KEY_FMT as LKM


@pytest.fixture(scope="session")
def redis_client():
    """
    Fixture to create a mock Redis client for testing.
    This uses a local Redis server running on the default port.
    """
    import fakeredis

    return fakeredis.FakeRedis()


@pytest.fixture(scope="session")
def true_redis():
    """
    Fixture to create a real redis client for tests that require it. Note that
    you must spin up a local redis instance to run this.
    :return:
    """
    import redis

    return redis.Redis(host="localhost", port=6379, decode_responses=True)


@pytest.fixture(scope="function")
def live_handler(true_redis):
    """
    Fixture to create a LabelHandler instance with a live Redis client.
    This is used for tests that require interaction with a real Redis server.
    """
    handler = LabelHandler(
        redis_client=true_redis, max_labels=2, runner_uuid="unit-test"
    )
    yield handler
    true_redis.flushdb()


@pytest.fixture(scope="function")
def label_handler(redis_client):
    """
    Fixture to create a LabelHandler instance with a mock Redis client.
    """
    handler = LabelHandler(
        redis_client=redis_client, max_labels=2, runner_uuid="unit-test"
    )
    yield handler
    redis_client.flushdb()


def test_add_label(label_handler):
    """
    Test adding a label to the LabelHandler.
    """
    label = "label-1"
    label_handler.add_label(label)

    assert label_handler.has_label(label), (
        f"Label '{label}' should be added to the handler."
    )
    assert len(label_handler) == 1, "There should be one label in the handler."
    # assert label_handler.redis.sismember(LKM.format(label=label), label), f"Label '{label}' should be in Redis set."

    label2 = "label-2"
    label_handler.add_label(label2)
    assert label_handler.has_label(label2), (
        f"Label '{label2}' should be added to the handler."
    )
    assert label_handler.has_label(label), (
        f"Label '{label}' should still be in the handler."
    )
    assert len(label_handler) == 2, "There should be two labels in the handler."


def test_remove_label(label_handler):
    """
    Test removing a label from the LabelHandler.
    """
    label_handler.add_label("label-1")
    label_handler.add_label("label-2")

    label_handler.remove_label("label-2")
    assert len(label_handler) == 1, (
        "There should be one label left after removal."
    )
    assert label_handler.has_label("label-1"), (
        "Label 'label-1' should still exist after removing 'label-2'."
    )


def test_clear_all(label_handler):
    """
    Test clearing all labels from the LabelHandler.
    """
    label_handler.add_label("label-1")
    label_handler.add_label("label-2")

    assert len(label_handler) == 2, (
        "There should be two labels before clearing."
    )

    label_handler.clear_all()

    assert len(label_handler) == 0, "There should be no labels after clearing."
    assert not label_handler.has_label("label-1"), (
        "Label 'label-1' should not exist after clearing."
    )
    assert not label_handler.has_label("label-2"), (
        "Label 'label-2' should not exist after clearing."
    )


def test_label_limit(label_handler):
    """
    Test that the label handler respects the maximum number of labels and the
    ordering.
    """
    for i in range(6):
        label = f"label-{i}"
        label_handler.add_label(label)

    assert len(label_handler) == 2, (
        "There should be only two labels in the handler due to the limit."
    )
    assert label_handler.has_label("label-4"), (
        "Label 'label-4' should be the second most recent."
    )
    assert label_handler.has_label("label-5"), (
        "Label 'label-5' should be the most recent."
    )


@pytest.mark.live_redis
def test_add_label_register(live_handler):
    """
    Test adding a label with a live Redis handler.
    """
    label = "live-label-1"
    live_handler.add_label(label)

    assert live_handler.has_label(label), (
        f"Label '{label}' should be added to the live handler."
    )
    assert len(live_handler) == 1, (
        "There should be one label in the live handler."
    )
    assert live_handler.redis.sismember(
        LKM.format(label=label), live_handler.runner_uuid
    ), f"Label '{label}' should be registered in Redis set."


@pytest.mark.live_redis
def test_remove_label_register(live_handler):
    """
    Test removing a label with a live Redis handler.
    """
    live_handler.add_label("live-label-1")
    live_handler.add_label("live-label-2")

    live_handler.remove_label("live-label-2")
    assert len(live_handler) == 1, (
        "There should be one label left after removal."
    )
    assert live_handler.has_label("live-label-1"), (
        "Label 'live-label-1' should still exist after removing 'live-label-2'."
    )
    assert not live_handler.redis.sismember(
        LKM.format(label="live-label-2"), live_handler.runner_uuid
    ), (
        "Label 'live-label-2' should not be registered in Redis set after removal."
    )

    assert live_handler.redis.sismember(
        LKM.format(label="live-label-1"), live_handler.runner_uuid
    ), (
        "Label 'live-label-1' should still be registered in Redis set after removal of 'live-label-2'."
    )


@pytest.mark.live_redis
def test_label_limit_redis(live_handler):
    """
    Test that the label handler with a live Redis respects the maximum number of labels and the ordering.
    """
    for i in range(6):
        label = f"live-label-{i}"
        live_handler.add_label(label)

    assert len(live_handler) == 2, (
        "There should be only two labels in the live handler due to the limit."
    )
    for i in range(4):
        label = f"live-label-{i}"
        key = LKM.format(label=label)
        assert not live_handler.redis.sismember(
            key, live_handler.runner_uuid
        ), (
            f"Label '{label}' should not be registered in Redis set after exceeding limit."
        )

    label = "live-label-4"
    assert live_handler.redis.sismember(
        LKM.format(label=label), live_handler.runner_uuid
    ), (
        f"Label '{label}' should be registered in Redis set as the second most recent."
    )

    label = "live-label-5"
    assert live_handler.redis.sismember(
        LKM.format(label=label), live_handler.runner_uuid
    ), f"Label '{label}' should be registered in Redis set as the most recent."
