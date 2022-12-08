import re
import sys
import json
import time
import logging
import datetime
# sys.path.insert(0, '.\config')

import requests

import config_env
import mongo_handler

from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


def crawl_link_cate(domain, config):
    # hàm crawl các link chỉ mục của website
    # truyền vào config của web
    # trả về list link chỉ mục của web
    try:
        browser = detect_type_crawl(config, config['url'])
        response = detect_type_response(browser, config)
        list_link_cate = detect_type_result(html_find_xpath(response, config['detect_link_cate']), config['detect_link_cate'])
        list_link_cate.append(domain)
        return make_full_link(domain, list_link_cate)
    except:
        logging.warning("Exception: ", exc_info=True)
        return False


def crawl_page():
    # hàm cào dữ liệu ở mặt các page
    # trả về danh sách các bản ghi cào được
    col_config = mongo_handler.connect_config_crawl_cmt()
    col_temp_db = mongo_handler.connect_temp_collection()
    col_toppaper = mongo_handler.connect_toppaper()
    config = col_config.find({})
    # lặp qua từng config để cào link chỉ mục của từng website
    for web_config in config:
        list_link_cate = crawl_link_cate(web_config['website'], web_config['crawl_link_cate'])
        config_crawl_page = web_config['crawl_page']
        # lặp qua từng trang chỉ mục để cào comment của từng chỉ mục
        for link_cate in list_link_cate:
            logging.info(f"Start crawl url: {link_cate}")
            type_crawl = config_crawl_page['type_crawl']
            browser = detect_type_crawl(config_crawl_page, link_cate)
            if browser == False:
                continue
            else:
                response = detect_type_response(browser, config_crawl_page)
            if response == False:
                continue
            else:
                if type_crawl == 1:
                    list_data = extract_comment_from_html(response, web_config['website'], web_config['type_object'], link_cate, config_crawl_page)
                elif type_crawl == 2:
                    list_data = extract_comment_from_browser(response, web_config['website'], web_config['type_object'], link_cate, config_crawl_page)

            if len(list_data) > 0:
                list_data = check_replace_data(list_data)
                mongo_handler.insert_col(col_toppaper, check_comment_gt_zero(list_data.copy()))
                list_data_old, list_data_new = check_data_in_DB(col_temp_db, list_data)
                if len(list_data_old) > 0:
                    mongo_handler.update_col(col_temp_db, list_data_old)
                if len(list_data_new) > 0:
                    mongo_handler.insert_col(col_temp_db, list_data_new)
                    
            else:
                logging.info(f"Not found data, url: {link_cate}")


def crawl_detail(url):
    # hàm cào comment của một url
    # truyền vào url
    # trả về comment của url đó
    config = check_config(url)  # check config của một url
    if not config:
        logging.warning(f"Not found config, url: {url}")
        return False
    else:
        config_crawl_detail = config['crawl_detail']
    if config_crawl_detail['type_crawl'] == 1:
        try:
            config_crawl_detail['api']
            id_post = get_id_post(url)
            comment = detect_type_api(id_post, config_crawl_detail)
            if comment == False:
                logging.warning(f"not found comment in url: {url}")
                return False
            else:
                return comment
        except:
            try:
                browser = detect_type_crawl(config_crawl_detail, url)
                response = detect_type_response(browser, config_crawl_detail)
                comment = html_find_xpath(response, config_crawl_detail['detect_comment'])
                comment = detect_type_result(comment, config_crawl_detail['detect_comment'])
                if comment == False:
                    logging.warning(f"not found comment in url: {url}")
                    return False
                else:
                    return comment
            except:
                return False
                
    elif config_crawl_detail['type_crawl'] == 2:
        pass


def detect_type_crawl(config, url):
    # xác định kiểu crawl: requests hoặc selenium.
    # trả về response sau khi request
    if config['type_crawl'] == 1:
        try:
            res = requests.get(url, headers=config_env.HEADER, timeout=config_env.TIMEOUT)
            return res
        except:
            logging.error("Exception:", exc_info=True)
            return False
    elif config['type_crawl'] == 2:
        try:
            options = Options()
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.headless = True
            browser = webdriver.Chrome(executable_path=config_env.PATH_CHROME_DRIVER, options=options)
            browser.implicitly_wait(config_env.TIMEOUT)
            browser.get(url)
            return browser
        except:
            logging.error("Exception:", exc_info=True)
            return False


def detect_type_response(response, config):
    # xác định kiểu response: html / json / browser(khi gọi selenium)
    # nhận response
    # trả về response đã được phân tích cú pháp
    if config['type_response'] == 1:
        if response.status_code == 200:
            _html = html.fromstring(response.text, 'lxml')
            return _html
        else:
            logging.warning(f"res.status_code: {response.status_code}")
            return False
    elif config['type_response'] == 2:
        if response.status_code == 200:
            _json = response.json()
            return _json
        else:
            logging.warning(f"res.status_code: {response.status_code}")
            return False
    elif config['type_response'] == 3:
        return response


def get_id_post(url):
    # hàm lấy id khỏi url
    # truyền vào url
    # trả về id
    id_post = re.findall(r'\d+', url)[-1]
    return id_post


def check_config(url):
    # hàm tìm kiếm config dựa vào url
    # nhận vào url chứa domain của website
    # trả về config của website đó
    col_config = mongo_handler.connect_config_crawl_cmt()
    list_web_config = col_config.find({})
    for config in list_web_config:
        domain = re.split('/|\\.', config['website'])[2]
        if domain in url:
            return config


def detect_type_api(id_post, config):
    # hàm xác định loại api:
    if config['api']['type_api'] == 1: # api với phương thức GET kèm theo param
        api, list_api = detect_type_param(config['api']['url'], id_post, config['api'])
        if len(list_api) == 0:
            try:
                browser = detect_type_crawl(config, api)
                response = detect_type_response(browser, config)
                comment = extract_comment_from_json(response, config)
                return comment
            except:
                return False
        else:
            try:
                max_comment = 0
                for api in list_api:
                    browser = detect_type_crawl(config, api)
                    response = detect_type_response(browser,config)
                    comment = extract_comment_from_json(response, config)
                    if comment <= max_comment:
                        continue
                    else:
                        max_comment = comment
                        break
                return max_comment
            except:
                return False
    elif config['api']['type_api'] == 2: # api với phương thức POST kèm theo param
        payload = config['api']['data']
        payload, list_api = detect_type_param(str(payload), id_post, config['api'])
        try:
            browser = requests.post(config['api']['url'], headers=config_env.HEADER, data=eval(payload), timeout=config_env.TIMEOUT)
            response = detect_type_response(browser, config)
            comment = extract_comment_from_json(response, config)
            return comment
        except:
            logging.error("Exception:", exc_info=True)
            return False
        

def extract_comment_from_html(response, domain, type_obj, resourceUrl, config):
    # hàm trích xuất comment từ response dạng html
    # config truyền vào là config đối tượng cần trích xuất
    # trả về list dữ liệu dạng json
    list_data = []
    list_obj_cmt = html_find_xpath(response, config['detect_comment'])
    if list_obj_cmt != None:
        list_obj_cmt = detect_type_result(list_obj_cmt, config['detect_comment'])
        for obj in list_obj_cmt:
            comment = re.findall(r'\d+', obj.text_content())
            check_link_post = False
            while comment and check_link_post == False:
                obj = obj.getparent()
                list_descendant = [node for node in obj.iterdescendants()]
                try:
                    for descendant in list_descendant:
                        link_post = html_find_xpath(descendant, config['detect_link'])
                        if link_post is None:
                            continue
                        link_post = detect_type_result(link_post, config['detect_link'])
                        if link_post:
                            link_post = make_full_link(domain, [link_post])
                            check_link_post = True
                            list_data.append(
                                {
                                    "type_doc":1, 
                                    "datetime": datetime.datetime.now(), 
                                    "resourceUrl":resourceUrl, "url":link_post[0], 
                                    "comment":int(comment[0]), 
                                    "type":type_obj
                                    }
                            )
                            break
                except:
                    pass
    else:
        logging.warning(f"Not found comment in url: {resourceUrl}")
    return list_data


def extract_comment_from_browser(response, domain, type_obj, resourceUrl, config):
    # hàm trích xuất comment từ trình duyệt khi gọi selenium
    # config truyền vào là config đối cần trích xuất
    # trả về list dữ liệu dạng json
    list_data = []
    try:
        list_obj_cmt = response.find_elements(By.XPATH, config['detect_comment']['xpath'])
        for obj in list_obj_cmt:
            comment = re.findall(r'\d+', obj.get_attribute('textContent'))
            check_link_post = False
            while comment and check_link_post == False:
                obj = obj.find_element(By.XPATH, '..')
                link_post = check_regex(config['detect_link']['re'], [str(obj.get_attribute('innerHTML'))])
                if link_post:
                    link_post = make_full_link(domain, link_post)
                    list_data.append({
                        "type_doc":1, 
                        "datetime": datetime.datetime.now(), 
                        "resourceUrl":resourceUrl, "url":link_post[0], 
                        "comment":int(comment[0]), 
                        "type":type_obj
                    })
                    break
    except:
        pass
    finally:
        response.close()
    return list_data


def extract_comment_from_json(json_obj, config):
    # hàm lấy comment từ json object
    # truyền vào đối tượng json, config crawl_detail
    # trả về số lượng comment
    new_obj = get_all_key_json(json_obj, {})  # làm phẳng đối tượng json nhiều bậc
    try:
        return new_obj[config['detect_comment']['key']]
    except:
        logging.warning(f"can't get comment from json obj: {json_obj}")
        return False
    

def get_all_key_json(json_obj, new_obj):
    # đệ quy để duyệt qua mọi key-val của json_obj
    # truyền vào json_obj
    # trả về dict 1 cấp
    for key, vals in json_obj.items():
        if isinstance(vals, str):
            new_obj[key] = vals
        elif isinstance(vals, int):
            new_obj[key] = vals
        elif isinstance(vals, dict):
            get_all_key_json(vals, new_obj)
        elif isinstance(vals, list):
            for temp in vals:
                get_all_key_json(temp, new_obj)
        else:
            new_obj[key] = ""
    return new_obj


def detect_type_result(result, config):
    # hàm phân loại dữ liệu nhận được
    # truyền vào dữ liệu nhận được. vd: "4"
    # trả về dữ liệu chuẩn. vd: 4 
    try:
        type_result = config['type_result'] 
        if type_result == 1:
            return elements_to_output(result, config)
        elif type_result == 2:
            return list_string_to_output(result, config)
        elif type_result == 3:
            return list_int_to_output(result, config)
        elif type_result == 4:
            return string_to_output(result, config)
        elif type_result == 5:
            return int_to_output(result, config)
        elif type_result == 6:
            return datetime_to_output(result, config)
        elif type_result == 7:
            return timestamp_to_output(result, config)
    except:
        return config


def html_find_xpath(html, config):
    # hàm tìm kiếm theo xpath
    # truyền vào html, config đối tượng tìm kiếm
    # trả về list kết quả tìm kiếm theo xpath 
    try:
        return html.xpath(config['xpath'])
    except:
        return None


def elements_to_output(obj, config):
    # truyền vào list element
    # trả về dữ liệu chuẩn
    if config['type_output'] == 1: 
        return obj
    elif config['type_output'] == 2:
        pass
    elif config['type_output'] == 3:
        pass
    elif config['type_output'] == 4:
        string = detect_type_find(remove_space("".join(obj)).strip(), config)
        return string
    elif config['type_output'] == 5:
        pass
    elif config['type_output'] == 6:
        pass
    pass


def list_string_to_output(obj, config):
    # truyền vào list string. vd: ["1", "2"]
    # trả về dữ liệu chuẩn. vd: [1, 2]
    if config['type_output'] == 2:
        list_output = []
        for i in obj:
            result = detect_type_find(i, config)
            list_output.append(result)
        return list_output
    elif config['type_output'] == 3:
        list_numb = detect_type_find(remove_space("".join(obj)).strip(), config)
        return [int(i) for i in list_numb]
    elif config['type_output'] == 4:
        string = detect_type_find(remove_space("".join(obj)).strip(), config)
        return "".join(string)
    elif config['type_output'] == 5:
        numb = detect_type_find(remove_space("".join(obj)).strip(), config)
        return int(numb)
    elif config['type_output'] == 6:
        time = detect_type_find(remove_space("".join(obj)).strip(), config)
        return detect_time_format(time, config)


def list_int_to_output(obj, config):
    pass


def string_to_output(obj, config):
    # convert string sang dữ liệu chuẩn. vd "4" -> 4
    if config['type_output'] == 3:
        obj = detect_type_find(remove_space("".join(obj).strip()), config)
        return [int(i) for i in obj]
    elif config['type_output'] == 4:
        obj = detect_type_find(remove_space("".join(obj)).strip(), config)
        return obj
    elif config['type_output'] == 5:
        obj = detect_type_find(remove_space("".join(obj)).strip(), config)
        return int(obj[0]) if isinstance(obj, list) else int(obj)
    elif config['type_output'] == 2:
        pass
    elif config['type_output'] == 6:
        obj = detect_type_find(remove_space("".join(obj)).strip(), config)
        time = detect_time_format(obj, config)


def int_to_output(obj, config):
    # convert int sang dữ liệu chuẩn
    if config['type_output'] == 5:
        return obj
    elif config['type_output'] == 2:
        pass
    elif config['type_output'] == 3:
        pass
    elif config['type_output'] == 4:
        pass
    elif config['type_output'] == 6:
        pass


def datetime_to_output(obj, config):
    pass


def timestamp_to_output(obj, config):
    # convert timestamp sang dữ liệu chuẩn
    if config['type_output'] == 6:
        obj = detect_type_find(obj, config)[0]
        obj = datetime.datetime.fromtimestamp(int(obj)/1000)
        return obj
    elif config['type_output'] == 3:
        pass
    elif config['type_output'] == 4:
        pass
    elif config['type_output'] == 5:
        pass
    elif config['type_output'] == 2:
        pass


def detect_type_find(obj, config):
    # hàm phân loại kiểu tìm kiếm
    # truyền vào đối tượng cần tìm kiếm
    # trả về đối tượng sau khi tìm kiếm
    if config['type_find'] == 1: 
        return obj
    elif config['type_find'] == 2: # sử dụng biểu thức chính quy để tìm kiếm
        return regex_extract(obj, config)


def regex_extract(obj, config):
    # hàm tìm kiếm theo biểu thức chính quy
    # truyền vào đối tượng tìm kiếm, config của đối tượng
    # trả về list kết quả sau khi tìm kiếm
    regex = config['re']
    result = re.findall(regex, obj)
    return result


def detect_time_format(time, config):
    # hàm định dạng lại thời gian
    if type(time) == list:
        time = time[0]
    time_format = config['time_format'].replace("days","%d").replace("months","%m").replace("years","%Y").replace("hours","%H").replace("minutes","%M").replace("seconds","%S").replace("microseconds", "%f")
    time = datetime.datetime.strptime(time, time_format)
    try:  # bổ sung thêm thời gian
        params = config['replace'].split('=')
        if params[0] == "years":
            time = time.replace(year=int(params[1]))
        elif params[0] == "months":
            time = time.replace(month=int(params[1]))
        elif params[0] == "days":
            time = time.replace(day=int(params[1]))
        elif params[0] == "hours":
            time = time.replace(hour=int(params[1]))
    except:
        logging.warning("Exception: ", exc_info=True)
    return time


def remove_space(string):
    return re.sub('\s+', ' ', string)


def detect_type_param(string, id_post, config):
    # hàm xác định loại replace param
    list_string = []
    for param in config['params']:
        if param['type_replace'] == 1: 
            new_string = string.replace(param['old_val'], id_post)
        elif param['type_replace'] == 2:
            for val in param['new_val']:
                list_string.append(new_string.replace(param['old_val'], val))
    return new_string, list_string


def check_replace_data(list_data):
    # hàm loại bỏ các bản ghi trùng lặp trước khi thêm vào db
    list_link_check = []
    for doc in list_data.copy():
        if doc['url'] not in list_link_check:
            list_link_check.append(doc['url'])
        else:
            list_data.remove(doc)
    return list_data


def check_data_in_DB(col, list_data):
    # hàm phân loại bản ghi: old thì sẽ dùng để cập nhật, new sẽ insert vào DB
    # truyền vào list bản ghi
    # trả về list bản ghi old và new
    list_link_old = []
    list_link_new = []
    for doc in list_data:
        if col.find_one({"url":doc['url']}):
            list_link_old.append(doc)
        else:
            list_link_new.append(doc)
    return list_link_old, list_link_new


def check_comment_gt_zero(list_data):
    # hàm kiểm tra comment > 0
    for doc in list_data.copy():
        if doc['comment'] > 0:
            pass
        else:
            list_data.remove(doc)
    return list_data


def make_full_link(domain, list_link):
    # hàm kiểm tra url và bổ sung thêm domain nếu url thiếu domain
    for i in range(len(list_link)):
        if list_link[i].startswith('http') == False:
            if list_link[i].startswith('/'):
                if domain.endswith('/'):
                    list_link[i] = domain.rsplit('/', 1)[0] + list_link[i]
                else:
                    list_link[i] = domain + list_link[i]
            else:
                if domain.endswith('/'):
                    list_link[i] = domain + list_link[i]
                else:
                    list_link[i] = domain + "/" + list_link[i]
        else:
            pass
    return list_link


def check_regex(regex, list_link):
    list_result = []
    for link in list_link:
        result = re.findall(regex, link)
        if result:
            list_result.append(result[0])
    return list(set(list_result))
    
        
def scroll_down(browser, param_scroll_down):
    if param_scroll_down == True:
        speed = 70
        current_scroll_position, new_height= 0, 1
        while current_scroll_position <= new_height:
            current_scroll_position += speed
            browser.execute_script("window.scrollTo(0, {});".format(current_scroll_position))
            new_height = browser.execute_script("return document.body.scrollHeight")
    else:
        pass


