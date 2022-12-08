import sys
import json
import logging
import datetime
sys.path.insert(0, '.\config')

import pymongo

import config_env



def connect_DB():
    # connect database
    client = pymongo.MongoClient(config_env.PATH_DB_MONGO)
    db = client["PaPer"]
    return db


def connect_temp_collection():
    # connect collection temp_collection
    db = connect_DB()
    col_temp_data = db["temp_collection"]
    return col_temp_data


def connect_toppaper():
    # connect collection toppaper
    db = connect_DB()
    col_toppaper = db["toppaper"]
    return col_toppaper


def connect_config_crawl_cmt():
    # connect collection config_crawl_cmt
    db = connect_DB()
    col_config = db["config_crawl_cmt"]
    return col_config


def find_config(col_config):
    # tìm kiếm config
    config = col_config.find({})
    return config


def get_all_doc(col):
    return col.find({})

def get_doc_today(col_temp_data, time_crawl_page):
    # tìm kiếm docs crawl chưa quá 24h trong temp_data
    # time_crawl_page: thời điểm bắt đầu crawl page, giúp lấy những docs trước khi crawl page
    return col_temp_data.find({"type" : config_env.TYPE_OBJECT, "type_doc":1, "last_check": {"$lt": time_crawl_page}})


def insert_col(col, list_data):
    # thêm dữ liệu vào col được truyền vào
    col.insert_many(list_data)


def update_col(col, list_doc):
    # cập nhật lại doc
    for doc in list_doc:
        doc['last_check'] = datetime.datetime.now()
        filter = {"url": doc['url']}
        vals = {"$set":doc}
        try:
            col.update_many(filter, vals)
        except:
            pass


def update_type_doc(col, list_doc):
    # for doc in list_doc:
    #     filter = {"url": doc['url']}
    #     vals = {"$set":doc}
    #     try:
    #         col.update_many(filter, vals)
    #     except:
    #         pass
    list_doc_update = []
    list_doc_over_time = []
    list_del = []
    for doc in list_doc:
        if (datetime.datetime.now() - doc['datetime']).days == 1:
            doc['type_doc'] = 2
            list_doc_update.append(doc)
        elif (datetime.datetime.now() - doc['datetime']).days > 3:
            list_doc_over_time.append(doc)
    if len(list_doc_update) > 0:
        update_col(col, list_doc_update)
    if len(list_doc_over_time) > 0:
        delete_doc(col, list_doc_over_time)


def delete_doc(col, list_data):
    for doc in list_data:
        col.delete_one(doc)


def update_config():
    col_config = connect_config_crawl_cmt()
    with open(config_env.PATH_CONFIG, 'r', encoding='utf-8') as read_config:
        configs = json.load(read_config)
    for config in configs:
        try:
            if col_config.find_one({"website":config['website']}):
                mapping_site = {"website":{"$regex":f"{config['website']}"}}
                update_vals = {"$set":config}
                col_config.update_one(mapping_site, update_vals)
            else:
                col_config.insert_one(config)
        except:
            print(config)
    logging.info("config updated !")
# update_config()

# def get_config():
#     col_config, col_temp_db, col_toppaper = connect_DB()
#     list_config = []
#     for config in col_config.find({}):
#         del config['_id']
#         list_config.append(config)
#     with open('config.json', 'w', encoding='utf-8') as write_config:
#         json.dump(list_config, write_config, ensure_ascii=False, indent=4)
# get_config()

# def check_update():
#     col_config, col_temp_db, col_toppaper, demo = connect_DB()
#     list_doc = demo.find({})
#     for doc in list_doc:
#         doc['comment'] = 10
#         update_col(demo, doc)
# check_update()
