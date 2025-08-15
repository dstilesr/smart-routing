import time
from loguru import logger

import logs
import tasks


if __name__ == "__main__":
    """
    Main Entrypoint for Worker. Sets up logging and starts the task runner,
    which listens for tasks to run.
    """
    logs.setup_logs()
    logger.info("Starting Worker...")
    with tasks.get_runner() as runner:
        logger.info("Worker running...")
        for _ in runner.listen():
            time.sleep(2)
