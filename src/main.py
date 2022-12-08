import sys
import time
import datetime
import logging
sys.path.insert(0, '..\config')

import mongo_handler
import crawl_comment
import config_env

import threading
from apscheduler.schedulers.blocking import BlockingScheduler
from logging.handlers import TimedRotatingFileHandler


def crawl_handler():
    # hàm xử lý crawl: chạy crawl page và crawl detail
    list_doc_new = []
    col_temp_db = mongo_handler.connect_temp_collection()
    col_toppaper = mongo_handler.connect_toppaper()
    time_crawl_page = datetime.datetime.now()
    all_doc = mongo_handler.get_all_doc(col_temp_db)
    crawl_comment.crawl_page()
    if datetime.datetime.now().time().hour % 6 == 0:
        mongo_handler.update_type_doc(col_temp_db, all_doc)
        list_doc_check_detail = mongo_handler.get_doc_today(col_temp_db, time_crawl_page)
        for doc in list_doc_check_detail:
            comment = crawl_comment.crawl_detail(doc['url'])
            if comment != False and comment > 0:
                del doc['_id']
                doc['comment'] = comment
                doc['last_check'] = datetime.datetime.now()
                list_doc_new.append(doc)
            else:
                pass
        if len(list_doc_new) > 0:
            mongo_handler.insert_col(col_toppaper, list_doc_new)
            logging.info(f"insert in toppaper successfull !")


def run_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(crawl_handler, 'interval', minutes=config_env.TIME_RUN_SCHEDULE, max_instances=config_env.MAX_JOBS)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Exiting...")
        sys.exit(0)


def set_log():
    rootlogger = logging.getLogger()
    rootlogger.setLevel(logging.DEBUG)
    file_log = TimedRotatingFileHandler(config_env.PATH_LOG,
                                       when="h",
                                       interval=1,
                                       backupCount=5)
    file_log.setLevel(logging.INFO)
    file_log.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    rootlogger.addHandler(file_log)


def read_config():
    list_config = mongo_handler.connect_config_crawl_cmt()
    return list_config


if __name__ == '__main__':
    set_log()
    mongo_handler.update_config()
    crawl_comment.crawl_page()
    crawl_handler()
