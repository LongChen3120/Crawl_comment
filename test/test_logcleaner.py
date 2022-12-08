import logging
import time
from logging.handlers import TimedRotatingFileHandler
#----------------------------------------------------------------------
def create_timed_rotating_log(path):
    """"""
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    
    handler = TimedRotatingFileHandler(path,
                                       when="s",
                                       interval=1,
                                       backupCount=5)
    logger.addHandler(handler)
    
    for i in range(20):
        logger.info(f"This is a test!: {i}")
        time.sleep(1)
#----------------------------------------------------------------------
if __name__ == "__main__":
    log_file = "timed_test.log"
    create_timed_rotating_log(log_file)