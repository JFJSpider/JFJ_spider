r'''
Author: wamgzwu
Date: 2024-12-30 15:45:40
LastEditors: wamgzwu wangzw26@outlook.com
LastEditTime: 2025-01-10 16:10:44
FilePath: \jfj_spider\dangdang_spider.py
Description: 当当采集
'''
from DrissionPage import Chromium, ChromiumOptions
import pymysql
from datetime import datetime
import requests
import base64
import time
import logging
import random
import argparse
import re
from connect2dbs import DatabaseManager
# from mysql.connector import Error 
BASE_URL = "https://search.dangdang.com/?key=%BE%FC%CA%C2&act=input"

class ModeLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})

    def process(self, msg, kwargs):
        # 在日志消息前添加模式标识（增量/全量）
        mode = self.extra.get('mode', '[Increment Mode]')  # 默认是增量模式
        return f"{mode} {msg}", kwargs
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dangdang_data_collection.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# 创建 logger 对象
logger = logging.getLogger(__name__)
# 检测是否登录, 如果没有则提示用户请登录(这里登录需要用户手动辅助)
def check_login(tab, adapter):
    if len(tab.eles("x://span[@id='nickname']//a[@dd_name='登录']", timeout = 1)) != 0: #需要登录
        time.sleep(2)
        tab.ele("x://span[@id='nickname']//a[@dd_name='登录']").click()
        while True:
            if len(tab.eles("x://ul[@class='ddnewhead_operate_nav']//li//a[@name='我的云书房']", timeout=1)) != 0:
                adapter.info("登录成功!")
                break
            time.sleep(2)
            adapter.warning("需要人工辅助登录!")
    else:
        adapter.info("检测到已登录, 开始采集数据")
def extract_price(price_string):
    # 正则表达式：匹配以"¥"开头的价格，捕获价格数字（包括小数）
    match = re.search(r'(\d+(\.\d+)?)', price_string)
    
    if match:
        # 提取匹配到的数字部分并转换为浮动数
        return match.group(1)
    else:
        return None  # 如果没有找到有效价格，返回 None
def img_to_base64(img_url, id):
    response = requests.get(img_url)
    time.sleep(1)
    # 确保请求成功
    if response.status_code == 200:
        # 将图片的二进制内容转换为Base64编码的字符串
        img_base64 = base64.b64encode(response.content).decode('utf-8')
        #将base64字符串转换回图片
        # with open(f'{id}.jpg', 'wb') as file:
        #     decoded_string = base64.b64decode(img_base64)
        #     file.write(decoded_string)
        return img_base64
    else:
        print('Failed to download the image')
        return ""
def crawl_data(mode: int):
    if mode == 0:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Increment Mode]'})
    else:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Full Mode]'})
    # mode为爬取模式, 1为全量爬取, 0为增量爬取
    co = ChromiumOptions()
    co.set_user_data_path('./data_dangdang')
    browser = Chromium(co)
    tab = browser.latest_tab
    exist_ids = select_AlldataID_from_database(adapter)
    # result_data = []
    adapter.info("START CRAWLING DATA FROM DANGDANG!")
    for page_num in range(1, 101):
        url = f"{BASE_URL}&page_index={page_num}"
        tab.get(url)
        tab.wait.doc_loaded()
        check_login(tab, adapter)
        # tab.scroll.to_bottom()
        result_li = tab.eles("x://div[@id='bd']//div[@class='col search_left']//div[@id='search_nature_rg']//ul//li")
        i = 0
        for li in result_li:
            result_data = []
            id = li.attr("id") # id
            if f'dangdang_{id}' in exist_ids:
                i += 1
                if mode == 1: # 全量模式
                    adapter.info(f"第{page_num}页第{i}条数据开始更新!")
                    # 采集价格和评论数
                    new_price_str = li.ele("x://p[@class='price']//span[@class='search_now_price']", timeout = 1).text
                    new_price = float(extract_price(new_price_str))
                    try:
                        new_comment_num_str = li.ele("x://p[@class='search_star_line']//a[@class='search_comment_num']", timeout = 1).text
                        new_comment_num = int(re.search(r'\d+', new_comment_num_str).group())
                    except Exception as e:
                        new_comment_num = None
                    update_data_to_database(f'dangdang_{id}', new_price, new_comment_num, adapter)
                else:
                    adapter.info(f"第{page_num}页第{i}条数据已存在,跳过!")
                continue
            try:
                publish_time_str = li.ele("x://p[@class='search_book_author']//span[2]").text.split('/')[-1]
                publish_time = datetime.strptime(publish_time_str, '%Y-%m-%d')
            except Exception as e:
                publish_time_str = None
            # title = li.ele("x://p[@name='title']").text # 标题
            exist_ids.add(f'dangdang_{id}')
            detail_url = li.ele("x://a").attr('href') # 详情页
            # 进入详情页采集数据
            new_tab = browser.new_tab(detail_url)
            time.sleep(random.randint(1, 3))
            new_tab.wait.doc_loaded()
            try:
                image_src = new_tab.ele("#largePic").attr('src')
            except Exception as e:
                adapter.error(f"该书籍没有图片!URL:{detail_url}")
                browser.close_tabs(new_tab)
                continue
            # 获取图片链接,并将其转为base64码
            img_base64 = img_to_base64(image_src, id)
            product_info = new_tab.ele("#product_info", timeout=1)
            title = product_info.ele("x://div[@class='name_info']//h1").text
            # 书的简介
            # introduce_book = product_info.ele("x://h2//span[@class='head_title_name']")
            # 作者
            try:
                author = product_info.ele("#author").ele("x://a", timeout=1).text
            except Exception as e:
                author = ""
            # 出版社
            try:
                publisher = product_info.ele("x://span[@ddt-area='003']//a", timeout=1).text
            except Exception as e:
                publisher = ""
            # 评论数
            try:
                comment_num = product_info.ele("x://span[@id='messbox_info_comm_num' and @style='']//a", timeout=1).text
            except Exception as e:
                # adapter.error(e)
                comment_num = None
            # 价格
            price_str = product_info.ele("#dd-price").text
            price = float(extract_price(price_str))
            describe = new_tab.ele("#detail_describe").eles("x://ul//li")
            ISBN = ""
            formats = ""
            paper = ""
            package = ""
            category = ""
            is_set = ""
            for li in describe:
                if 'ISBN' in li.text:
                    ISBN = li.text.split('：')[-1]
                if '开 本' in li.text:
                    formats = li.text.split('：')[-1]
                if '纸 张' in li.text:
                    paper = li.text.split('：')[-1]
                if '包 装' in li.text:
                    package = li.text.split('：')[-1]
                if '分类' in li.text:
                    category = li.text.split('：')[-1]
                if '套装' in li.text:
                    is_set = li.text.split('：')[-1]
            
            # 编辑推荐
            try:
                editor_recommendations_parent = new_tab.ele("#abstract", timeout=1)
                if editor_recommendations_parent.ele("x://div[@class='descrip']//span[@id='abstract-all']").text == '':
                    editor_recommendations = editor_recommendations_parent.ele("x://div[@class='descrip']").text
                else:
                    editor_recommendations = editor_recommendations_parent.ele("x://div[@class='descrip']//span[@id='abstract-all']").text
            except Exception as e:
                editor_recommendations = ""
            # 内容简介
            try:
                content_intro_parent = new_tab.ele("#content", timeout=1)
                if content_intro_parent.ele("x://div[@class='descrip']//span[@id='content-all']").text == '':
                    content_intro = content_intro_parent.ele("x://div[@class='descrip']").text
                else:
                    content_intro = content_intro_parent.ele("x://div[@class='descrip']//span[@id='content-all']").text
            except Exception as e:
                content_intro = ""
            # 作者简介
            try:
                author_intro_parent = new_tab.ele("#authorIntroduction", timeout=1)
                if author_intro_parent.ele("x://div[@class='descrip']//span[@id='authorIntroduction-all']").text == '':
                    author_intro = author_intro_parent.ele("x://div[@class='descrip']").text
                else:
                    author_intro = author_intro_parent.ele("x://div[@class='descrip']//span[@id='authorIntroduction-all']").text
            except Exception as e:
                author_intro = ""
            # 目录
            try:
                catalogue_parent = new_tab.ele("#catalog", timeout=1)
                if catalogue_parent.ele("x://div[@class='descrip']//span[@id='catalog-show-all']").text == '':
                    catalogue = catalogue_parent.ele("x://div[@class='descrip']").text
                else:
                    catalogue = catalogue_parent.ele("x://div[@class='descrip']//span[@id='catalog-show-all']").text
            except Exception as e:
                catalogue = ""
            # 前言
            try:
                preface_parent = new_tab.ele("#preface", timeout=1)
                if preface_parent.ele("x://div[@class='descrip']//span[@id='preface-show-all']").text == '':
                    preface = preface_parent.ele("x://div[@class='descrip']").text
                else:
                    preface = preface_parent.ele("x://div[@class='descrip']//span[@id='preface-show-all']").text
            except Exception as e:
                preface = ""
            # print(f"ISBN: {ISBN}, format: {formats}, paper: {paper}, package: {package}, category: {category}")
            data = {
                "str_id": f'dangdang_{id}',
                "title": title,
                "author": author,
                "author_intro": author_intro,
                "publisher": publisher,
                "publish_time": publish_time_str,
                "catalogue": catalogue,
                "preface": preface,
                "content_intro": content_intro,
                "ISBN": ISBN,
                "subcategory": category,
                "price": float(price),
                "evaluation_number": int(comment_num) if comment_num is not None else None,
                "package": package,
                "is_set": is_set, # 套装
                "format": formats, # 开本
                "paper": paper,
                "editor_recommendations": editor_recommendations,
                "image_url": image_src,
                "image_base64": img_base64,
                "page_url": detail_url,
                "data_type": "当当",
                "data_status": "1",
                "book_type": "1"
            }
            result_data.append(data)
            browser.close_tabs(new_tab)
            i += 1
            # print(f"第{page_num}页第{i}条数据采集!")
            adapter.info(f"第{page_num}页第{i}条数据采集!")
            save_data_to_database(result_data, adapter)
        # print(f"第{page_num}页采集完成")
        adapter.info(f"第{page_num}页采集完成")
        # save_data_to_database(result_data)
def select_AlldataID_from_database(adapter):
    db_manager = DatabaseManager()
    db_manager.connect()
    query = f"SELECT str_id FROM rs_correct_resources"
    results = db_manager.execute_query(query)
    if results is not None:
        allll = set([row[0] for row in results])
        return allll
    else:
        adapter.info("数据查询为空!")
        return []
def select_data_from_database_by_id(dataid, adapter):
    db_manager = DatabaseManager()
    db_manager.connect()
    query = f"SELECT * FROM rs_correct_resources WHERE str_id = '{dataid}'"
    results = db_manager.execute_query(query)
    if results is not None:
        adapter.info(f"数据查询已存在!")
# 全量采集时如果数据存在则对其进行更新, 目前只用更新价格和评论数
def update_data_to_database(id, price, evaluation_number, adapter):
    db_manager = DatabaseManager()
    db_manager.connect()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    columns  = ['"price"', '"update_time"']
    values = [price, current_time]
    if evaluation_number is not None:
        columns.append('"evaluation_number"')
        values.append(evaluation_number)
    db_manager.update('rs_correct_resources', columns, values, 'str_id', id)
    adapter.info(f"数据[{id}]已存在, 已对其进行更新!")
    
def save_data_to_database(result_data, adapter):
    db_manager = DatabaseManager()
    db_manager.connect()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data_dict = {
        "str_id": result_data[0]["str_id"],
        "title": result_data[0]["title"],
        "author": result_data[0]["author"],
        "author_intro": result_data[0]["author_intro"],
        "publish": result_data[0]["publisher"],
        "publish_time": result_data[0]["publish_time"],
        "catalogue": result_data[0]["catalogue"],
        "preface": result_data[0]["preface"],
        "content_intro": result_data[0]["content_intro"],
        "ISBN": result_data[0]["ISBN"],
        "subcategory": result_data[0]["subcategory"],
        "price": result_data[0]["price"],
        "evaluation_number": result_data[0]["evaluation_number"],
        "packages": result_data[0]["package"],
        "is_set": result_data[0]["is_set"],
        "format": result_data[0]["format"],
        "paper": result_data[0]["paper"],
        "editor_recommendations": result_data[0]["editor_recommendations"],
        "image_url": result_data[0]["image_url"],
        "base64_url": result_data[0]["image_base64"],
        "page_url": result_data[0]["page_url"],
        "data_type": result_data[0]["data_type"],
        "data_status": result_data[0]["data_status"],
        "create_time": current_time,
        "update_time": current_time,
        "deleted": 0,
        "book_type": result_data[0]["book_type"]
    }
    columns = []
    values = []
    for key,value in data_dict.items():
        if value is not None:
            columns.append('"'+key+'"')
            values.append(value)
    #去除末尾,
    db_manager.insert('rs_correct_resources', columns, values)
    adapter.info(f"数据已成功插入到表中")
    db_manager.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="当当网数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集")
    args = parser.parse_args()
    mode = args.m
    crawl_data(mode)