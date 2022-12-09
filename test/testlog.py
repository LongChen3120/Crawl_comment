import logging, time
import os, psutil
import func_temp
from logging.handlers import TimedRotatingFileHandler

def set_log(logger_name, log_file):
    root_logger = logging.getLogger(logger_name)
    root_logger.setLevel(logging.DEBUG)
    timed_rotating = TimedRotatingFileHandler(
        log_file,
        when='s',
        interval=1,
        backupCount=5
    )
    timed_rotating.setLevel(logging.INFO)
    timed_rotating.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root_logger.addHandler(timed_rotating)


def get_mem():
    process = psutil.Process(os.getpid())
    logger_mem.warning(int(process.memory_info().rss)/1024)


def main():
    tong = 0
    for i in range(10):
        time.sleep(1)
        tong += i
        log_main.info(f"tong: {tong}")
        func_temp.a()
        get_mem()

if __name__ == '__main__':
    set_log('log_main', './test/log_main.log')
    set_log('log_mem', './test/log_mem.log')
    log_main = logging.getLogger('log_main')
    logger_mem = logging.getLogger('log_mem')
    main()

