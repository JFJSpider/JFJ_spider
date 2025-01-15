'''
Author: wamgzwu
Date: 2024-12-30 15:45:40
LastEditors: wamgzwu wangzw26@outlook.com
LastEditTime: 2025-01-03 21:11:15
FilePath: \jfj_spider\dangdang_spider.py
Description: 当当采集
'''
from DrissionPage import Chromium, ChromiumOptions
import psycopg2
from datetime import datetime
import requests
import base64
import time
import logging
import random
import argparse
import re

# from mysql.connector import Error
BASE_URL = "https://search.jd.com/search?keyword=%E5%86%9B%E4%BA%8B"


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
        logging.FileHandler("jingdong_data_collection.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# 创建 logger 对象
logger = logging.getLogger(__name__)


# 检测是否登录, 如果没有则提示用户请登录(这里登录需要用户手动辅助)
def check_login(tab, adapter):
    if len(tab.eles("x://li[@id='ttbar-login']//a[@class='link-login']", timeout=1)) != 0:  # 需要登录
        time.sleep(2)
        tab.ele("x://li[@id='ttbar-login']//a[@class='link-login']").click()
        while True:
            if len(tab.eles("x://li[@id='ttbar-login']//a[@class='nickname']", timeout=1)) != 0:
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
        # 将base64字符串转换回图片
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
    co.set_user_data_path('./data_jingdong')
    browser = Chromium(co)
    tab = browser.latest_tab
    # result_data = []
    adapter.info("START CRAWLING DATA FROM JINGDONG!")
    for page_num in range(1, 200):
        url = f"{BASE_URL}&pvid=229f11fc142c4b7ba72789b76f72ddc5&cid2=3276&isList=0&page={page_num}"
        tab.get(url)
        tab.wait.doc_loaded()
        check_login(tab, adapter)
        # tab.scroll.to_bottom()
        result_li = tab.eles("x://div[@id='J_searchWrap']//div[@id='J_goodsList']//ul//li")
        i = 0
        for li in result_li:
            result_data = []
            id = li.attr("data-sku")  # id
            if select_data_from_database(f'jingdong_{id}', adapter):
                i += 1
                if mode == 1:  # 全量模式
                    adapter.info(f"第{page_num}页第{i}条数据开始更新!")
                    # 采集价格和评论数
                    new_price_str = li.ele("x://div[@class='p-price']//i", timeout=1).text
                    new_price = float(extract_price(new_price_str))
                    try:
                        new_comment_num_str = li.ele("x://div[@class='p-commit']//a",
                                                     timeout=1).text
                        match = re.search(r'(\d+\.?\d*)', new_comment_num_str)
                        if match:
                            number = float(match.group(1))  # 提取数字部分并转换为浮点数
                            if '万' in new_comment_num_str:  # 判断是否包含 "万"
                                new_comment_num = int(number * 10000)  # 转换为整数后乘以 10000
                            else:
                                new_comment_num = int(number)  # 如果没有 "万"，直接取整数
                        else:
                            new_comment_num = None  # 如果没有匹配到数字，设置为 0
                    except Exception as e:
                        new_comment_num = None
                    update_data_to_database(f'jingdong_{id}', new_price, new_comment_num, adapter)
                else:
                    adapter.info(f"第{page_num}页第{i}条数据已存在,跳过!")
                continue
            # try:
            #     publish_time_str = li.ele("x://p[@class='search_book_author']//span[2]").text.split('/')[-1]
            #     publish_time = datetime.strptime(publish_time_str, '%Y-%m-%d')
            # except Exception as e:
            #     publish_time = None
            # title = li.ele("x://p[@name='title']").text # 标题
            detail_url = li.ele("x://div[@class='p-img']//a").attr('href')  # 详情页
            # 进入详情页采集数据
            new_tab = browser.new_tab(detail_url)
            time.sleep(random.randint(1, 3))
            new_tab.wait.doc_loaded()
            # 采集类别
            try:
                item_first = new_tab.ele("x://div[@class='crumb fl clearfix']//div[@class='item first']//a", timeout=1).text
                items = new_tab.eles("x://div[@class='crumb fl clearfix']//div[@class='item']", timeout=1)
                item_texts = [item.text for item in items]
                subcategory = f"{item_first} > {' > '.join(item_texts)}"
            except Exception as e:
                subcategory = None
            try:
                image_src = new_tab.ele("x://div[@id='spec-n1']//img").attr('src')
                # 获取图片链接,并将其转为base64码
                img_base64 = img_to_base64(image_src, id)
            except Exception as e:
                image_src = None
                img_base64 = None
            product_info = new_tab.ele("x://div[@class='itemInfo-wrap']", timeout=1)
            # 书名
            title = product_info.ele("x://div[@class='sku-name']").text
            # 书的简介
            # introduce_book = product_info.ele("x://h2//span[@class='head_title_name']")
            # 作者
            try:
                authors = product_info.eles("x://div[@class='news']//div[@class='p-author']//a", timeout=1)
                author_names = [author.text for author in authors]
                author = "，".join(author_names)
            except Exception as e:
                author = ""
            # # 出版社
            # try:
            #     publisher = product_info.ele("x://span[@ddt-area='003']//a", timeout=1).text
            # except Exception as e:
            #     publisher = ""
            # 评论数
            try:
                comment_num_str = product_info.ele("x://div[@id='comment-count']//a", timeout=1).text
                comment_match = re.search(r'(\d+\.?\d*)', comment_num_str)
                if comment_match:
                    number = float(comment_match.group(1))  # 提取数字部分并转换为浮点数
                    if '万' in comment_num_str:  # 判断是否包含 "万"
                        comment_num = int(number * 10000)  # 转换为整数后乘以 10000
                    else:
                        comment_num = int(number)  # 如果没有 "万"，直接取整数
                else:
                    comment_num = None
            except Exception as e:
                # adapter.error(e)
                comment_num = None
            # 价格(获取不到为空，全量模式下重新获取)
            try:
                price_str = product_info.ele(
                    "x://div[@class='summary-price J-summary-price']//div[@class='dd']//span//span[2]", timeout=1).text
                price = float(extract_price(price_str))
            except Exception as e:
                price = None

            describe = new_tab.eles("x://div[@class='p-parameter']//ul[@class='parameter2 p-parameter-list']//li")
            ISBN = ""
            publisher = ""
            edition = ""
            brand = ""
            packages = ""
            series_titles = ""
            format = ""
            use_paper = ""
            product_code = ""
            language = ""
            pages = ""
            words = ""
            publish_time = ""
            number_sets = ""
            for li in describe:
                if '出版社' in li.text:
                    publisher = li.text.split('：')[-1]
                if '版次' in li.text:
                    edition = li.text.split('：')[-1]
                if 'ISBN' in li.text:
                    ISBN = li.text.split('：')[-1]
                if '商品编码' in li.text:
                    product_code = li.text.split('：')[-1]
                if '品牌' in li.text:
                    brand = li.text.split('：')[-1]
                if '包装' in li.text:
                    packages = li.text.split('：')[-1]
                if '丛书名' in li.text:
                    series_titles = li.text.split('：')[-1]
                if '开本' in li.text:
                    format = li.text.split('：')[-1]
                if '出版时间' in li.text:
                    publish_time_str = li.text.split('：')[-1]
                    publish_time = datetime.strptime(publish_time_str, '%Y-%m-%d')
                if '用纸' in li.text:
                    use_paper = li.text.split('：')[-1]
                if '页数' in li.text:
                    pages = li.text.split('：')[-1]
                if '套装数量' in li.text:
                    number_sets = li.text.split('：')[-1]
                if '字数' in li.text:
                    words = li.text.split('：')[-1]
                    if words == 'null':
                        words = 0  # 如果是 'null'，设置为 0
                    else:
                        # 判断是否包含“千字”或“万字”
                        if '千字' in words:
                            # 提取前面的数字并转换为整数，然后乘以1000
                            num = int(words.split('千字')[0])
                            words = num * 1000
                        elif '万字' in words:
                            # 提取前面的数字并转换为整数，然后乘以10000
                            num = int(words.split('万字')[0])
                            words = num * 10000
                        else:
                            # 如果没有“千字”或“万字”，尝试直接转换为整数
                            words = int(words)
                if '正文语种' in li.text:
                    language = li.text.split('：')[-1]

            try:
                # 尝试定位 '内容简介' 元素
                content_intro_location_1 = new_tab.ele('text:内容简介', timeout=1)
                content_intro = content_intro_location_1.after('tag:div', 1).text
            except Exception as e:
                content_intro_location_1 = None
                content_intro = None

            try:
                if content_intro_location_1 is None:
                    content_intro_location_2 = new_tab.ele('text:内容提要', timeout=1)
                    content_intro = content_intro_location_2.after('tag:p', 1).text
            except Exception as e:
                content_intro_location_2 = None
                content_intro = None

            try:
                editor_recommendations_location = new_tab.ele('text:编辑推荐', timeout=1)
                editor_recommendations = editor_recommendations_location.after('tag:div', 1).text
                if not editor_recommendations:
                    editor_recommendations = editor_recommendations_location.after('tag:p', 1).text
                if not editor_recommendations:
                    editor_recommendations = editor_recommendations_location.after('tag:tr', 1).text
            except Exception as e:
                editor_recommendations = None


            try:
                author_intro_location_1 = new_tab.ele('text:作者简介', timeout=1)
                author_intro =author_intro_location_1.after('tag:div', 1).text
                if not author_intro:
                    author_intro = author_intro_location_1.after('tag:p', 1).text
                if not author_intro:
                    author_intro = author_intro_location_1.after('tag:tr', 1).text
            except Exception as e:
                author_intro_location_1 = None
                author_intro = None

            try:
                if author_intro_location_1 is None:
                    author_intro_location_2 = new_tab.ele('text:作者介绍', timeout=1)
                    author_intro = author_intro_location_2.after('tag:p', 1).text
            except Exception as e:
                author_intro_location_2 = None
                author_intro = None

            try:
                preface_location_1 = new_tab.ele('text:前言', timeout=1)
                preface = preface_location_1.after('tag:div', 1).text
                if not preface:
                    preface = preface_location_1.after('tag:p', 1).text
                if not preface:
                    prefaceo = preface_location_1.after('tag:tr', 1).text
            except Exception as e:
                preface_location_1 = None
                preface = None

            try:
                if  preface_location_1 is None:
                    preface_location_2 = new_tab.ele('text:序言', timeout=1)
                    preface = preface_location_2.after('tag:p', 1).text
            except Exception as e:
                preface_location_2 = None
                preface = None

            try:
                catalogue_location = new_tab.ele('text:目录', timeout=1)
                catalogue = catalogue_location.after('tag:div', 1).text
                if not catalogue:
                    catalogue = catalogue_location.after('tag:p', 1).text
                if not catalogue:
                    catalogue = catalogue_location.after('tag:tr', 1).text
            except Exception as e:
                catalogue = None


            # print(f"ISBN: {ISBN}, format: {formats}, paper: {paper}, package: {package}, category: {category}")
            data = {
                "str_id": f'jingdong_{id}',
                "title": title,
                "author": author,
                "author_intro": author_intro,
                "subcategory": subcategory,
                "publisher": publisher,
                "publish_time": publish_time,
                "catalogue": catalogue,
                "preface": preface,
                "content_intro": content_intro,
                "ISBN": ISBN,
                "price": float(price) if price is not None else None,
                "evaluation_number": int(comment_num) if comment_num is not None else None,
                "packages": packages,
                "series_titles": series_titles,
                "format": format,
                "use_paper": use_paper,
                "edition": edition,
                "brand": brand,
                "product_code": product_code,
                "language": language,
                "pages": pages,
                "words": words,
                "number_sets": number_sets,
                "editor_recommendations": editor_recommendations,
                "image_url": image_src,
                "image_base64": img_base64,
                "page_url": detail_url,
                "data_type": "京东",
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


def select_data_from_database(dataid, adapter):
    try:
        # 连接数据库
        connection = psycopg2.connect(
            host="10.101.221.240",  # 数据库主机地址
            port="54321",
            user="reslib",  # 用户名
            password="1qaz3edc123!@#",  # 密码
            dbname="reslib"  # 数据库名称
        )
        if connection:
            print("成功连接到数据库")
            # 查询数据
            cursor = connection.cursor()
            select_query = f"SELECT * FROM rs_correct_resources WHERE str_id = %s"
            cursor.execute(select_query, (dataid,))
            result = cursor.fetchone()
            connection.close()
            return result
    except Exception as e:
        print("数据库连接失败:", e)
    finally:
        # 确保游标和连接被关闭
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()


# 全量采集时如果数据存在则对其进行更新, 目前只用更新价格和评论数
def update_data_to_database(id, price, evaluation_number, adapter):
    try:
        # 连接到MySQL数据库
        connection = psycopg2.connect(
            host="10.101.221.240",  # 数据库主机地址
            port="54321",
            user="reslib",  # 用户名
            password="1qaz3edc123!@#",  # 密码
            dbname="reslib"  # 数据库名称
        )
        if connection:
            # print("成功连接到数据库")
            # 查询所有需要更新的数据
            cursor = connection.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # 构建SQL更新语句
            if evaluation_number is None:
                update_query = """
                           UPDATE rs_correct_resources 
                           SET price = %s, update_time = %s 
                           WHERE str_id = %s
                       """
                cursor.execute(update_query, (price, current_time, id))
            else:
                update_query = """
                           UPDATE rs_correct_resources 
                           SET price = %s, evaluation_number = %s, update_time = %s 
                           WHERE str_id = %s
                       """
                cursor.execute(update_query, (price, evaluation_number, current_time, id))
            connection.commit()
            adapter.info("数据已更新到数据库")
    except Exception as e:
        adapter.error(e)
    finally:
        # 确保游标和连接被正确关闭
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
            # print("数据库连接已关闭")


def save_data_to_database(result_data, adapter):
    try:
        # 连接到MySQL数据库
        connection = psycopg2.connect(
            host="10.101.221.240",  # 数据库主机地址
            port="54321",
            user="reslib",  # 用户名
            password="1qaz3edc123!@#",  # 密码
            dbname="reslib"  # 数据库名称
        )
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_dict = {
            "str_id": result_data[0]["str_id"],
            "title": result_data[0]["title"],
            "author": result_data[0]["author"],
            "subcategory": result_data[0]["subcategory"],
            "author_intro": result_data[0]["author_intro"],
            "publish": result_data[0]["publisher"],
            "publish_time": result_data[0]["publish_time"],
            "catalogue": result_data[0]["catalogue"],
            "preface": result_data[0]["preface"],
            "content_intro": result_data[0]["content_intro"],
            "\"ISBN\"": result_data[0]["ISBN"],
            "price": result_data[0]["price"],
            "evaluation_number": result_data[0]["evaluation_number"],
            "packages": result_data[0]["packages"],
            "series_titles": result_data[0]["series_titles"],
            "format": result_data[0]["format"],
            "use_paper": result_data[0]["use_paper"],
            "edition": result_data[0]["edition"],
            "brand": result_data[0]["brand"],
            "product_code": result_data[0]["product_code"],
            "language": result_data[0]["language"],
            "pages": result_data[0]["pages"],
            "words": result_data[0]["words"],
            "number_sets": result_data[0]["number_sets"],
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
        if connection:
            cursor = connection.cursor()
            str_sql_head = 'INSERT INTO rs_correct_resources'
            str_sql_middle = ""
            str_sql_value = ""
            value_list = []
            for key, value in data_dict.items():
                if value:
                    str_sql_middle = str_sql_middle + key + ","
                    str_sql_value += "%s,"
                    value_list.append(value)
            # 去除末尾,
            str_sql_middle = str_sql_middle.rstrip(",")
            str_sql_value = str_sql_value.rstrip(",")

            str_sql_middle = f"({str_sql_middle})"
            str_sql_value = f"VALUES ({str_sql_value})"

            all_sql_str = f"{str_sql_head} {str_sql_middle} {str_sql_value}"
            # adapter.info(f'sql语句:{all_sql_str}')
            cursor.execute(all_sql_str, value_list)
            connection.commit()
            adapter.info(f"数据已成功插入到表中")

    except Exception as e:
        # print("错误:", e)
        adapter.error(e)

    finally:
        # 确保游标和连接被正确关闭
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
            # print("数据库连接已关闭")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="京东数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集")
    args = parser.parse_args()
    mode = args.m
    crawl_data(mode)