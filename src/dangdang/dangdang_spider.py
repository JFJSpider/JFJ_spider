'''
Author: wamgzwu
Date: 2024-12-30 15:45:40
LastEditors: wamgzwu wangzw26@outlook.com
LastEditTime: 2025-01-03 11:08:29
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
        logging.FileHandler("data_collection.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# 创建 logger 对象
logger = logging.getLogger(__name__)
# 检测是否登录, 如果没有则提示用户请登录(这里登录需要用户手动辅助)
def check_login(tab, adapter):
    if len(tab.eles("x://span[@id='nickname']//a[@dd_name='登录']", timeout = 3)) != 0: #需要登录
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
            if select_data_from_database(f'dangdang_{id}', adapter):
                i += 1
                if mode == 1: # 全量模式
                    adapter.info(f"第{page_num}页第{i}条数据开始更新!")
                    # 采集价格和评论数
                    new_price = float(li.ele("x://p[@class='price']//span[@class='search_now_price']", timeout = 2).text.split('¥')[-1])
                    try:
                        new_comment_num_str = li.ele("x://p[@class='search_star_line']//a[@class='search_comment_num']", timeout = 2).text
                        new_comment_num = int(re.search(r'\d+', new_comment_num_str).group())
                    except Exception as e:
                        new_comment_num = 'NULL'
                    update_data_to_database(f'dangdang_{id}', new_price, new_comment_num, adapter)
                else:
                    adapter.info(f"第{page_num}页第{i}条数据已存在,跳过!")
                continue
            try:
                publish_time_str = li.ele("x://p[@class='search_book_author']//span[2]").text.split('/')[-1]
                publish_time = datetime.strptime(publish_time_str, '%Y-%m-%d')
            except Exception as e:
                publish_time = 'NULL'
            # title = li.ele("x://p[@name='title']").text # 标题
            detail_url = li.ele("x://a").attr('href') # 详情页
            # 进入详情页采集数据
            new_tab = browser.new_tab(detail_url)
            time.sleep(random.randint(1, 3))
            new_tab.wait.doc_loaded()
            
            image_src = new_tab.ele("#largePic").attr('src')
            # 获取图片链接,并将其转为base64码
            img_base64 = img_to_base64(image_src, id)
            product_info = new_tab.ele("#product_info", timeout=3)
            title = product_info.ele("x://div[@class='name_info']//h1").text
            # 书的简介
            # introduce_book = product_info.ele("x://h2//span[@class='head_title_name']")
            # 作者
            try:
                author = product_info.ele("#author").ele("x://a", timeout=3).text
            except Exception as e:
                author = ""
            # 出版社
            try:
                publisher = product_info.ele("x://span[@ddt-area='003']//a", timeout=3).text
            except Exception as e:
                publisher = ""
            # 评论数
            try:
                comment_num = product_info.ele("#messbox_info_comm_num", timeout=2).ele("x://a", timeout=2).text
            except Exception as e:
                adapter.error(e)
                comment_num = 'NULL'
            # 价格
            price = product_info.ele("#dd-price").text.split('¥')[-1]
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
                editor_recommendations_parent = new_tab.ele("#abstract", timeout=4)
                if editor_recommendations_parent.ele("x://div[@class='descrip']//span[@id='abstract-all']").text == '':
                    editor_recommendations = editor_recommendations_parent.ele("x://div[@class='descrip']").text
                else:
                    editor_recommendations = editor_recommendations_parent.ele("x://div[@class='descrip']//span[@id='abstract-all']").text
            except Exception as e:
                editor_recommendations = ""
            # 内容简介
            try:
                content_intro_parent = new_tab.ele("#content", timeout=4)
                if content_intro_parent.ele("x://div[@class='descrip']//span[@id='content-all']").text == '':
                    content_intro = content_intro_parent.ele("x://div[@class='descrip']").text
                else:
                    content_intro = content_intro_parent.ele("x://div[@class='descrip']//span[@id='content-all']").text
            except Exception as e:
                content_intro = ""
            # 作者简介
            try:
                author_intro_parent = new_tab.ele("#authorIntroduction", timeout=4)
                if author_intro_parent.ele("x://div[@class='descrip']//span[@id='authorIntroduction-all']").text == '':
                    author_intro = author_intro_parent.ele("x://div[@class='descrip']").text
                else:
                    author_intro = author_intro_parent.ele("x://div[@class='descrip']//span[@id='authorIntroduction-all']").text
            except Exception as e:
                author_intro = ""
            # 目录
            try:
                catalogue_parent = new_tab.ele("#catalog", timeout=4)
                if catalogue_parent.ele("x://div[@class='descrip']//span[@id='catalog-show-all']").text == '':
                    catalogue = catalogue_parent.ele("x://div[@class='descrip']").text
                else:
                    catalogue = catalogue_parent.ele("x://div[@class='descrip']//span[@id='catalog-show-all']").text
            except Exception as e:
                catalogue = ""
            # 前言
            try:
                preface_parent = new_tab.ele("#preface", timeout=4)
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
                "publish_time": publish_time,
                "catalogue": catalogue,
                "preface": preface,
                "content_intro": content_intro,
                "ISBN": ISBN,
                "subcategory": category,
                "price": float(price),
                "evaluation_number": int(comment_num),
                "package": package,
                "is_set": is_set, # 套装
                "format": formats, # 开本
                "paper": paper,
                "editor_recommendations": editor_recommendations,
                "image_url": image_src,
                "image_base64": img_base64,
                "page_url": detail_url,
                "data_source": "当当网",
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
def select_data_from_database(dataid, adapter):
    try:
        # 连接到MySQL数据库
        connection = pymysql.connect(
            host="localhost",  # 仅填写主机名
            port=3306,  # 指定端口
            user="root",
            password="123456",
            database="reslib",
            charset="utf8mb4"  # 设置字符集为utf8mb4
        )
        if connection.open:
            # print("成功连接到数据库")
            # 查询数据
            cursor = connection.cursor()
            select_query = f"SELECT * FROM rs_correct_resources WHERE str_id='{dataid}'"
            cursor.execute(select_query)
            result = cursor.fetchone()
            connection.close()
            return result
    except Exception as e:
        print("数据库连接失败:", e)
    finally:
        if connection.open:
            cursor.close()
            connection.close()
            # print("数据库连接已关闭")
# 全量采集时如果数据存在则对其进行更新, 目前只用更新价格和评论数
def update_data_to_database(id, price, evaluation_number, adapter):
    try:
        # 连接到MySQL数据库
        connection = pymysql.connect(
            host="localhost",  # 仅填写主机名
            port=3306,  # 指定端口
            user="root",
            password="123456",
            database="reslib",
            charset="utf8mb4"  # 设置字符集为utf8mb4
        )
        if connection.open:
            # print("成功连接到数据库")
            # 查询所有需要更新的数据
            cursor = connection.cursor()
            update_query = f"UPDATE rs_correct_resources SET price={price}, evaluation_number={evaluation_number} WHERE str_id='{id}'"
            cursor.execute(update_query)
            connection.commit()
            adapter.info("数据已更新到数据库")
    except Exception as e:
        adapter.error(e)
    finally:
        if connection.open:
            cursor.close()
            connection.close()
def save_data_to_database(result_data, adapter):
    try:
        # 连接到MySQL数据库
        connection = pymysql.connect(
            host="localhost",  # 仅填写主机名
            port=3306,  # 指定端口
            user="root",
            password="123456",
            database="reslib",
            charset="utf8mb4"  # 设置字符集为utf8mb4
        )

        if connection.open:
            # print("成功连接到数据库")
            # print(f"共{len(result_data)}条数据")
            count = 0 #计数
            for row in result_data:
                # 插入数据的SQL语句
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                insert_query = f"""
                INSERT INTO rs_correct_resources (
                    str_id, title, all_title, series_titles, author, author_intro, editor, publish, 
                    publish_time, publish_place, catalogue, preface, content_intro, abstracts, ISBN, 
                    language, keyword, document_type, subcategory, product_features, price, 
                    evaluation_number, People_buy, sellers_number, packages, brand, product_code, 
                    is_set, format, paper, collection_information, editor_recommendations, pages, 
                    words, edition, version, additional_version, Illustration_url, image_url, image_base64, page_url, 
                    data_source, data_type, data_status, creator, create_time, updater, update_time, 
                    deleted, tenant_id, may, book_type
                ) VALUES (
                    '{row['str_id']}', '{row['title']}', '', '', '{row['author']}', '{row['author_intro']}',
                    '', '{row['publisher']}', '{row['publish_time']}', '', '{row['catalogue']}', 
                    '{row['preface']}', '{row['content_intro']}', '', '{row['ISBN']}', '', '', '', '{row['subcategory']}', '', {row['price']}, {row['evaluation_number']}, NULL, NULL, '{row['package']}', 
                    '', '', '{row['is_set']}', '{row['format']}', '{row['paper']}', '', '{row['editor_recommendations']}', 
                    NULL, NULL, NULL, '', '', '', '{row['image_url']}', '{row['image_base64']}',
                    '{row['page_url']}', '{row['data_source']}', '', '', '', NULL, 
                    '', NULL, 0, NULL, NULL, '{row['book_type']}'
                );
                """
                cursor = connection.cursor()
                cursor.execute(insert_query)
                # 提交到数据库
                connection.commit()
                count += 1
                # print(f"第{count}条数据已成功插入到表中")
                adapter.info(f"第{count}条数据已成功插入到表中")

    except Exception as e:
        # print("错误:", e)
        adapter.error(e)

    finally:
        if connection.open:
            cursor.close()
            connection.close()
            # print("数据库连接已关闭")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="当当网数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集")
    args = parser.parse_args()
    mode = args.m
    crawl_data(mode)