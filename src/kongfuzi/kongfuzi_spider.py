'''
Author:liuzhiwei
Date: 2024-12-30 15:45:40
LastEditors: liuzhiwei zwliu27424@gmail.com
LastEditTime: 2025-01-03 21:11:15
FilePath: \jfj_spider\kongfuzi\kongfuzi_spider.py
Description: 孔夫子旧书网
'''

""""
三种模式
第一种为全量爬取，如果原始数据库中没有该网站的任何数据，则爬取所有数据
第二种为增量爬取，如果执行了第一次全量爬取，则可以开始进行增量爬取，获取更新数据
第三种为更新数据库，将数据库中的网页数据进行更新

"""

import os
import threading
import queue
import json
from DrissionPage import SessionPage, SessionOptions,Chromium, ChromiumOptions
from tqdm import tqdm
import pymysql
from datetime import datetime
import requests
import base64
import time
import logging
import random
import argparse
import re
from time import sleep

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

# 写入数据


# 检测是否登录, 如果没有则提示用户请登录(这里登录需要用户手动辅助)
def is_logged_in(tab):
    # 假设 cookies() 返回一个包含字典的列表
    now_url = tab.url  # 确认返回值的格式
    if "login" not in now_url:
        return True
    else:
        return False

def check_login(tab, adapter,timeout=60,):

    if not is_logged_in(tab):
        adapter.info("需要登录，正在等待用户操作...")

        start_time = time.time()
        while not is_logged_in(tab):
            time.sleep(0.5)  # 减少等待时间，提升响应速度
            if time.time() - start_time > timeout:
                adapter.info("登录超时，请重新尝试！")
        
        adapter.info("登录成功！")
    else:
        adapter.info("检测到已登录, 开始采集数据")


def img_to_base64(img_url, adapter):
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
        adapter.info('Failed to download the image')
        return ""
    

def get_child_urls(base_url, page, mode,adapter,output="./output/", ):
    # 检查输出目录
    if not os.path.exists(output):
        os.makedirs(output)
    # 定义文件路径
    all_file_path = os.path.join(output, "child_all.txt")
    old_file_path = os.path.join(output, "child_old.txt")
    new_file_path = os.path.join(output, "child_new.txt")

    if mode == 0:
        # 全量爬取
        adapter.info("第一次全量爬取")
        child_urls = []

        adapter.info("开始爬取所有页...")
        current_page = 1  # 从第1页开始
        with open(all_file_path, "w") as out_file:
            while True:
                adapter.info(f"正在爬取第 {current_page} 页的子网页...")
                url = base_url + str(current_page)
                adapter.info(url)
                # 发送 POST 请求
                page.get(url)
                page_json = page.json
                # 检查返回数据
                if not page_json or "data" not in page_json or "itemResponse" not in page_json["data"] or not page_json["data"]["itemResponse"]["list"]:
                    adapter.info(f"第 {current_page} 页无数据，爬取结束。")
                    break
                # 提取 mids 并生成子网页链接
                mids = [item['mid'] for item in page_json['data']['itemResponse']['list']]
                for mid in mids:
                    child_url = f"https://item.kongfz.com/book/{mid}.html"
                    child_urls.append(child_url)
                    out_file.write(child_url + "\n")
                current_page += 1  # 继续爬取下一页

        # 备份全量爬取结果为旧文件
        # 将all_file_path重命名为old_file_path
        if os.path.exists(all_file_path):
            os.rename(all_file_path, old_file_path)

        adapter.info("爬取完成!")
        adapter.info(f"共爬取 {len(child_urls)} 个子网页，结果保存在 {all_file_path}")

    elif mode == 1:
        # 增量爬取
        adapter.info("增量爬取模式")
        # 读取上次的结果
        if not os.path.exists(old_file_path):
            adapter.info("未找到旧文件，无法进行增量爬取，请先执行全量爬取！")
            return []
        # 读取旧数据
        with open(old_file_path, "r") as f:
            child_old_urls = set(line.strip() for line in f.readlines())
        adapter.info(f"读取到 {len(child_old_urls)} 条旧数据")
        child_urls = []
        current_page = 1
        success = True
        # 开始增量爬取
        with open(new_file_path, "w") as out_file:
            while success:
                try:
                    adapter.info(f"正在更新第 {current_page} 页的子网页...")
                    url = base_url + str(current_page)
                    adapter.info(url)

                    # 发送 POST 请求
                    page.get(url)
                    page_json = page.json

                    # 检查返回数据
                    if not page_json or "data" not in page_json or "itemResponse" not in page_json["data"] or not page_json["data"]["itemResponse"]["list"]:
                        adapter.info(f"第 {current_page} 页无数据，爬取结束。")
                        success = False
                        break

                    # 提取 mids 并生成子网页链接
                    mids = [item['mid'] for item in page_json['data']['itemResponse']['list']]
                    for mid in mids:
                        child_url = f"https://item.kongfz.com/book/{mid}.html"
                        if child_url not in child_old_urls:
                            child_urls.append(child_url)
                            out_file.write(child_url + "\n")

                    current_page += 1

                except Exception as e:
                    adapter.info(f"爬取第 {current_page} 页时出现错误: {e}")
                    success = False
            
        # 将增量爬取结果写入新文件
        with open(all_file_path, "w") as all_file:
            all_urls = child_old_urls.union(child_urls)
            for url in all_urls:
                all_file.write(url + "\n")
        if len(child_urls) == 0:    
            adapter.info("增量爬取无数据，爬取结束。")
            return []
        adapter.info("增量爬取完成!")
        adapter.info(f"新增 {len(child_urls)} 条子网页，结果保存在 {new_file_path}")

    else:
        # 更新数据库
        adapter.info("更新数据库模式")
        with open(all_file_path, "r") as f:
            child_urls = [line.strip() for line in f.readlines()]

    return child_urls



from datetime import datetime

def standardize_date(date_str):
    """
    将输入的日期字符串转换为 YYYY-MM-DD 格式。
    如果只给出年份，则默认为该年的1月1日。
    如果给出年份和月份，则默认为该月的第一天。
    
    参数:
    date_str (str): 原始日期字符串，可能的格式为 "YYYY" 或 "YYYY-MM"

    返回:
    str: 标准化的日期字符串 "YYYY-MM-DD"
    """
    try:
        # 尝试按照年-月-日的格式解析日期
        date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            # 尝试按照年-月的格式解析日期
            date = datetime.strptime(date_str, "%Y-%m")
        except ValueError:
            try:
                # 尝试按照年的格式解析日期
                date = datetime.strptime(date_str, "%Y")
            except ValueError:
                # 如果都不匹配，返回错误信息
                return "Invalid date format"
        
        # 如果只有年和月，设置日期为月的第一天
        date = date.replace(day=1)
    
    # 返回格式化的日期字符串
    return date.strftime("%Y-%m-%d")

def get_elems_multithreaded(url, page,cookies):
    # 固定字段
    main_data = {}
    ID = url.split('/')[-1].replace(".html","")
    main_data['str_id'] = "kfz"+ID
    main_data['data_type'] = 'kongfuzi'  # 数据类型
    main_data['data_status'] = 1        # 数据状态：已提交
    main_data['book_type'] = 1          # 书类型：图书
    main_data['subcategory'] = "图书>军事"  # 存入数据字典
    main_data['page_url'] = url
    # 尝试提取标题
    try:
        title = page.ele('@@tag()=h1').text  # 定位页面中 <h1> 标签的文本内容
    except:
        title = ''  # 如果提取失败，设置为默认空字符串
    main_data['title'] = title  # 将标题赋值到数据字典中

    # 尝试提取作者信息
    try:
        author = page.ele('@@tag()=span@@text():作者').next().text  # 定位包含“作者”的 <span> 标签，并获取其下一个兄弟节点的文本
    except:
        author = ''  # 提取失败则设置为空
    main_data['author'] = author  # 存入数据字典

    ##纸张paper
    # 尝试提取纸张信息
    try:
        paper = page.ele('@@tag()=span@@text():纸张').next().text  # 定位包含“作者”的 <span> 标签，并获取其下一个兄弟节点的文本
    except:
        paper = ''  # 提取失败则设置为空
    main_data['paper'] = paper  # 存入数据字典

    # 尝试提取出版社信息
    try:
        publish = page.ele('@@tag()=span@@text():出版社').next().text  # 定位“出版社”标签，并获取其下一个兄弟节点的文本
    except:
        publish = ''  # 提取失败设置为空
    main_data['publish'] = publish  # 存入数据字典

    # 尝试提取出版时间
    try:
        publish_time = page.ele('@@tag()=span@@text():出版时间').next().text  # 定位“出版时间”标签并提取下一个兄弟节点的文本
        publish_time = standardize_date(publish_time)
    except:
        publish_time = ''  # 提取失败设置为空
    main_data['publish_time'] = publish_time  # 存入数据字典

    #尝试提取页数
    try:
        pages = page.ele('@@tag()=span@@text():页数').next().text  # 定位“页数”标签并提取下一个兄弟节点的文本
        pages = pages.replace('[^\\d]', '', regex=True)
    except:
        pages = ''  # 提取失败设置为空
    main_data['pages'] = pages  # 存入数据字典

    #尝试提取字数
    try:    
        words = page.ele('@@tag()=span@@text():字数').next().text  # 定位“字数”标签并提取下一个兄弟节点的文本
        words = words.replace('千字', '000', regex=True)
        words = words.replace('[^\\d]', '', regex=True)

    except:
        words = ''  # 提取失败设置为空
    main_data['words'] = words  # 存入数据字典

    # 尝试提取版次
    try:
        edition = page.ele('@@tag()=span@@text():版次').next().text  # 定位“版次”标签并提取下一个兄弟节点的文本
        edition = edition.replace('[^\\d]', '', regex=True)
    except:
        edition = ''  # 提取失败设置为空
    main_data['edition'] = edition  # 存入数据字典
    #尝试提取丛书
    try:
        series_titles = page.ele('@@tag()=span@@text():丛书').next().text  # 定位“丛书”标签并提取下一个兄弟节点的文本
    except:
        series_titles = ''  # 提取失败设置为空
    main_data['series_titles'] = series_titles  # 存入数据字典


    # 尝试提取 ISBN 号
    try:
        ISBN = page.ele('@@tag()=span@@text():ISBN').next().text  # 定位“ISBN”标签并提取其下一个兄弟节点的文本
    except:
        ISBN = ''  # 提取失败设置为空
    main_data['ISBN'] = ISBN  # 存入数据字典

    #尝试提取价格
    try:
        price = page.ele('@@tag()=span@@text():定价').next().text  # 定位“价格”标签并提取下一个兄弟节点的文本
        price = price.replace('[^\\d.]', '', regex=True)
    except:
        price = ''  # 提取失败设置为空
    main_data['price'] = price  # 存入数据字典
    # 尝试提取开本信息
    try:
        format = page.ele('@@tag()=span@@text():开本').next().text  # 定位“开本”标签并提取下一个兄弟节点的文本
    except:
        format = ''  # 提取失败设置为空
    main_data['format'] = format  # 存入数据字典


    # 尝试提取装帧方式
    try:
        binding = page.ele('@@tag()=span@@text():装帧').next().text  # 定位“装帧”标签并提取下一个兄弟节点的文本
    except:
        binding = ''  # 提取失败设置为空
    main_data['binding'] = binding  # 存入数据字典

    # 尝试提取购买人数
    try:
        People_buy = page.ele('@@tag()=span@@text():买过').text  # 定位包含“人买过”的 <span> 标签，并获取其文本
        People_buy = People_buy.replace('[^\\d]', '', regex=True)
    except:
        People_buy = ''  # 提取失败设置为空
    main_data['People_buy'] = People_buy  # 存入数据字典
    
    #尝试提取简介
    try:    
        content_intro = page.ele('@@tag()=li@@class:jianjie').text  # 定位“简介”标签并提取文本
    except:
        content_intro = ''  # 提取失败设置为空
    main_data['content_intro'] = content_intro  # 存入数据字典

    #尝试提取在售商家数
    try:    
        sellers_number = page.ele('x:///html/body/div[1]/div[3]/div[2]/div[1]/ul/li[1]').text  # 定位“在售商家数”标签并提取下一个兄弟节点的文本
        sellers_number = sellers_number.replace('[^\\d]', '', regex=True)
    except:
        sellers_number = ''  # 提取失败设置为空
    main_data['sellers_number'] = sellers_number  # 存入数据字典

    #尝试提取图片
    try:    
        image_url = page.ele('@@tag()=div@@class=detail-img').ele('@@tag()=a').link  # 定位“图片”标签并提取下一个兄弟节点的文本
    except:
        image_url = ''  # 提取失败设置为空
    main_data['image_url'] = image_url  # 存入数据字典

    #尝试提取封面
    base64_url = ''
    if image_url != '':
        try:    
            base64_url = img_to_base64(image_url)
        except:
            base64_url = ''  # 提取失败设置为空
    main_data['base64_url'] = base64_url  # 存入数据字典

    return main_data

def connect_mysql(db_config,adapter):
    """
    连接到 MySQL 数据库，如果指定的数据库不存在，则创建该数据库。
    """
    try:
        # 尝试连接到指定数据库
        connection = pymysql.connect(**db_config)
        adapter.info(f"Connected to database: {db_config['database']}")
        return connection
    except pymysql.err.OperationalError as e:
        # 检查错误是否是因为数据库不存在
        if e.args[0] == 1049:  # 错误代码 1049: Unknown database
            adapter.info(f"Database '{db_config['database']}' does not exist. Creating it...")
            # 创建不带 database 的配置
            db_config_no_db = db_config.copy()
            db_config_no_db.pop('database')
            # 连接到 MySQL，不指定数据库
            connection = pymysql.connect(**db_config_no_db)
            try:
                with connection.cursor() as cursor:
                    # 创建数据库
                    cursor.execute(f"CREATE DATABASE `{db_config['database']}` CHARACTER SET {db_config['charset']}")
                    adapter.info(f"Database '{db_config['database']}' created successfully.")
                connection.commit()
            finally:
                connection.close()
            # 重新连接到新创建的数据库
            db_config_with_db = db_config.copy()
            return pymysql.connect(**db_config_with_db)
        else:
            # 如果是其他错误，抛出异常
            raise
        
# 指数增长的延迟函数
def exponential_backoff(retry_count, base_delay=0.5, factor=2):
    """
    计算指数增长的延迟时间。
    :param retry_count: 当前重试次数
    :param base_delay: 基础延迟时间（秒）
    :param factor: 指数增长因子
    :return: 计算后的延迟时间
    """
    return base_delay * (factor ** retry_count)
def get_data(page, url, cookies,adapter):
    """    
    参数:
        page: 页面对象，提供 `get` 方法。
        url: 目标 URL。
        cookies: 请求使用的 cookies。
        
    返回:
        成功采集到的数据，如果失败返回 None。
    """
    retry_count = 0
    MAX_RETRIES = 10

    while retry_count <= MAX_RETRIES:
        try:
            # 尝试获取页面数据
            page.get(url=url, cookies=cookies)
            data = get_elems_multithreaded(url, page, adapter)
            adapter.info(f"成功采集数据: {url}")
            return data  # 成功时直接返回数据
        except Exception as e:
            retry_count += 1
            adapter.info(f"Error processing {url}: {e}, retry {retry_count}")
            
            # 检查是否已达到最大重试次数
            if retry_count > MAX_RETRIES:
                adapter.info(f"Failed after {MAX_RETRIES} retries: {url}")
                return None  # 返回 None 表示失败

            # 计算延迟时间并等待
            delay = exponential_backoff(retry_count)
            adapter.info(f"重试第 {retry_count} 次，等待 {delay:.2f} 秒后重试...")
            time.sleep(delay)
def get_mysql_config():
        # 连接到 MySQL 数据库
    # MySQL 数据库连接信息
    db_config = {
        'host': '127.0.0.1',       # 数据库地址
        'port': 3306,              # 数据库端口
        'user': 'root',            # 用户名
        'password': '12345678',    # 密码
        'database': 'book',     # 数据库名称
        'charset': 'utf8mb4'       # 编码
    }
    # 创建表信息 SQL
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS kongfuzi (
        id INT AUTO_INCREMENT PRIMARY KEY,  -- 主键，自动递增
        str_id VARCHAR(50) NOT NULL,        -- 书籍唯一标识符
        data_type VARCHAR(50) NOT NULL,     -- 数据类型（固定为 kongduzi）
        data_status INT NOT NULL,           -- 数据状态（1: 已提交）
        book_type INT NOT NULL,             -- 书籍类型（1: 图书）
        subcategory VARCHAR(255) NOT NULL,  -- 分类（如 图书>军事）
        page_url VARCHAR(255) NOT NULL,     -- 页面 URL
        title VARCHAR(255),                 -- 书籍标题
        author VARCHAR(255),                -- 作者
        paper VARCHAR(255),                 -- 纸张
        publish VARCHAR(255),               -- 出版社
        publish_time VARCHAR(255),          -- 出版时间
        pages VARCHAR(50),                  -- 页数
        words VARCHAR(50),                  -- 字数
        edition VARCHAR(50),                -- 版次
        series_titles VARCHAR(255),         -- 丛书
        ISBN VARCHAR(50),                   -- ISBN 号
        price VARCHAR(50),                  -- 价格
        format VARCHAR(50),                 -- 开本
        binding VARCHAR(255),               -- 装帧
        People_buy VARCHAR(50),             -- 购买人数
        content_intro TEXT,                 -- 简介
        sellers_number VARCHAR(50),         -- 在售商家数
        image_url VARCHAR(255),             -- 图片 URL
        base64_url LONGTEXT                     -- 封面（base64 编码）
    );
    """
    #插入格式
    insert_sql = """
        INSERT INTO kongfuzi (str_id, data_type, data_status, book_type, subcategory, page_url, title, author, paper, publish, 
                            publish_time, pages, words, edition, series_titles, ISBN, price, format, binding, People_buy, 
                            content_intro, sellers_number, image_url, base64_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    return db_config, create_table_sql, insert_sql

def main(child_urls,adapter):
    #参数
    #代理
    #base_url
    #output_path



    #三、获取登录信息
    browser = Chromium()
    tab = browser.latest_tab
    tab.get('https://item.kongfz.com/book/41478539.html')
    check_login(tab,adapter)
    cookies_str = tab.cookies().as_str()
    
    adapter.info(f"登录信息为{cookies_str}")
    cookies = {cookie.split('=')[0].strip(): cookie.split('=')[1].strip() for cookie in cookies_str.split(';')}
    adapter.info(f"登录信息为{cookies}")


    browser.close_tabs(tab)

    so = SessionOptions()
    page = SessionPage(session_or_options=so)  # 用该配置创建页面对象

    # 五、写入结果
    db_config, create_table_sql, insert_sql = get_mysql_config()
    with connect_mysql(db_config,adapter) as connection:
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            for url in child_urls:
                result = get_data(page,url,cookies=cookies,adapter=adapter)
                if result:
                    # 准备数据（将每条记录转换为元组）
                    data_to_insert = [
                        (
                            result['str_id'],
                            result['data_type'],
                            result['data_status'],
                            result['book_type'],
                            result['subcategory'],
                            result['page_url'],
                            result['title'],
                            result['author'],
                            result['paper'],
                            result['publish'],
                            result['publish_time'],
                            result['pages'],
                            result['words'],
                            result['edition'],
                            result['series_titles'],
                            result['ISBN'],
                            result['price'],
                            result['format'],
                            result['binding'],
                            result['People_buy'],
                            result['content_intro'],
                            result['sellers_number'],
                            result['image_url'],
                            result['base64_url']
                        ) 
                    ]
                    # 批量插入数据
                    cursor.executemany(insert_sql, data_to_insert)
                    connection.commit()
                    adapter.info(f"{cursor.rowcount} records inserted successfully.")
                else:
                    adapter.info("没有获取到数据")

if __name__ == "__main__":



    parser = argparse.ArgumentParser(description="孔夫子网数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为全量采集, 1为增量量采集,2为更新数据库")
    args = parser.parse_args()
    mode = args.m
    # 一、启动爬虫
    output = "./output/"
    base_url = "https://search.kongfz.com/pc-gw/search-web/client/pc/bookLib/category/list?redirectFrom=booklib_category&catId=24&actionPath=catId&page="
    so = SessionOptions()
    page = SessionPage(session_or_options=so)  # 用该配置创建页面对象

    if mode == 0:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Full Mode]'})
    elif mode == 1:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Incremental Mode]'})
    else:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Database Update Mode]'})
    #二、按照爬取模式获取child_urls
    if mode in [0, 1, 2]:
        # 根据 mode 调用对应的功能
        child_urls = get_child_urls(base_url, page, mode=mode,adapter=adapter,output=output)
    else:
        raise ValueError("Invalid mode. Expected 0 (全量爬取), 1 (增量爬取), or 2 (更新数据库).")
    
    if len(child_urls) == 0:
        adapter.info("没有需要更新/爬取的数据")
    else:
        main(child_urls,adapter)
        



