import os
import sys
import time
import queue
import datetime
import logging
import asyncio
sys.path.insert(0, '.\config')

import mongo_handler
import crawl_comment
import config_env

import psutil
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from logging.handlers import TimedRotatingFileHandler


def crawl_handler():
    # hàm xử lý crawl: chạy crawl page và crawl detail
    log_ram.warning(f"Start run cheduler ===================== RAM USE: {crawl_comment.get_ram()} MB =====================")
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
            if comment != False:
                del doc['_id']
                doc['comment'] = comment
                doc['last_check'] = datetime.datetime.now()
                list_doc_new.append(doc)
            else:
                pass
        if len(list_doc_new) > 0:
            mongo_handler.insert_col(col_toppaper, crawl_comment.check_comment_gt_zero(list_doc_new))
            log_main.info(f"insert in toppaper successfull !")


def run_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(crawl_handler, 'interval', minutes=config_env.TIME_RUN_SCHEDULE, max_instances=config_env.MAX_JOBS)
    scheduler.start()
    while True:
        time.sleep(3600)


def set_log(logger_name, log_file):
    rootlogger = logging.getLogger(logger_name)
    rootlogger.setLevel(logging.DEBUG)
    timed_rotating = TimedRotatingFileHandler(log_file,
                                       when="h",
                                       interval=1,
                                       backupCount=5
                                       )
    timed_rotating.setLevel(logging.INFO)
    timed_rotating.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    rootlogger.addHandler(timed_rotating)


def read_config():
    list_config = mongo_handler.connect_config_crawl_cmt()
    return list_config


if __name__ == '__main__':
    set_log(config_env.NAME_LOG_1, config_env.PATH_LOG_1)
    set_log(config_env.NAME_LOG_2, config_env.PATH_LOG_2)
    log_main = logging.getLogger(config_env.NAME_LOG_1)
    log_ram = logging.getLogger(config_env.NAME_LOG_2)

    # mongo_handler.update_config()
    crawl_comment.crawl_page()
    # run_scheduler()