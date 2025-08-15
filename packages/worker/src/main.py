import time
from loguru import logger

import tasks


if __name__ == "__main__":
    logger.info("Starting Worker...")
    with tasks.TaskRunner() as runner:
        logger.info("Worker running...")
        for _ in runner.listen():
            print("Task received")
            time.sleep(2)
