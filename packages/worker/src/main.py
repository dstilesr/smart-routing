import time
from loguru import logger

import tasks


if __name__ == "__main__":
    logger.info("Starting Worker...")
    with tasks.TaskRunner() as runner:
        logger.info("Worker running...")
        while True:
            #: TODO - temp logic for testing
            time.sleep(5)
