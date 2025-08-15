import time
from loguru import logger

import tasks


if __name__ == "__main__":
    logger.info("Starting Worker...")
    with tasks.get_runner() as runner:
        logger.info("Worker running...")
        for _ in runner.listen():
            time.sleep(2)
