import csv
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
import pymysql



def img_to_base64(img_url):
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

def get_elems_multithreaded(url, page,cookies):
    # 固定字段
    main_data = {}
    ID = url.split('/')[-1].replace(".html","")
    main_data['str_id'] = ID
    main_data['data_type'] = 'kongduzi'  # 数据类型
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
    print(author)

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
    except:
        publish_time = ''  # 提取失败设置为空
    main_data['publish_time'] = publish_time  # 存入数据字典

    #尝试提取页数
    try:
        pages = page.ele('@@tag()=span@@text():页数').next().text  # 定位“页数”标签并提取下一个兄弟节点的文本
    except:
        pages = ''  # 提取失败设置为空
    main_data['pages'] = pages  # 存入数据字典

    #尝试提取字数
    try:    
        words = page.ele('@@tag()=span@@text():字数').next().text  # 定位“字数”标签并提取下一个兄弟节点的文本
    except:
        words = ''  # 提取失败设置为空
    main_data['words'] = words  # 存入数据字典

    # 尝试提取版次
    try:
        edition = page.ele('@@tag()=span@@text():版次').next().text  # 定位“版次”标签并提取下一个兄弟节点的文本
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

def connect_mysql(db_config):
    """
    连接到 MySQL 数据库，如果指定的数据库不存在，则创建该数据库。
    """
    try:
        # 尝试连接到指定数据库
        connection = pymysql.connect(**db_config)
        print(f"Connected to database: {db_config['database']}")
        return connection
    except pymysql.err.OperationalError as e:
        # 检查错误是否是因为数据库不存在
        if e.args[0] == 1049:  # 错误代码 1049: Unknown database
            print(f"Database '{db_config['database']}' does not exist. Creating it...")
            # 创建不带 database 的配置
            db_config_no_db = db_config.copy()
            db_config_no_db.pop('database')
            # 连接到 MySQL，不指定数据库
            connection = pymysql.connect(**db_config_no_db)
            try:
                with connection.cursor() as cursor:
                    # 创建数据库
                    cursor.execute(f"CREATE DATABASE `{db_config['database']}` CHARACTER SET {db_config['charset']}")
                    print(f"Database '{db_config['database']}' created successfully.")
                connection.commit()
            finally:
                connection.close()
            # 重新连接到新创建的数据库
            db_config_with_db = db_config.copy()
            return pymysql.connect(**db_config_with_db)
        else:
            # 如果是其他错误，抛出异常
            raise


def writresult(data_to_insert,cursor):
    # 写入数据库
    try:
        # 创建表
        cursor.execute(create_table_sql)
        print("Table created successfully.")
        # 插入数据
        
        # 批量插入数据
        cursor.executemany(insert_sql, data_to_insert)
        # 提交事务    
        connection.commit()
        print("数据已成功写入 MySQL 数据库！")
    except pymysql.MySQLError as e:
        print(f"MySQL 错误: {e}")
        connection.rollback()


if __name__ == "__main__":
    url = "https://item.kongfz.com/book/52629823.html"
    cookies_str = "reciever_area=1006000000; shoppingCartSessionId=4815f316f2a06bf0345194cfd2a7e757; kfz_uuid=b32934cf-4956-4528-a0ee-df8a01b3f893; _c_WBKFRo=bBt7ZnKVJ1EJKf4PDptnQxuzvi4KElAWOqefEArZ; PHPSESSID=2d7b9859da682186882c93054bce73b3bb017521; kfz_trace=b32934cf-4956-4528-a0ee-df8a01b3f893|22656857|d2294e0f9c149cbe|; acw_tc=1a0c650a17363045682784891e004581078c2e7a806a165b3cd121996604e6"
    # 将 cookies 字符串解析为字典
    cookies = {cookie.split('=')[0].strip(): cookie.split('=')[1].strip() for cookie in cookies_str.split(';')}
    so = SessionOptions()
    page = SessionPage(session_or_options=so)  # 用该配置创建页面对象
    page.get(url,cookies=cookies)
    result = get_elems_multithreaded(url, page,cookies)
    print(result)

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

    if result:
        with connect_mysql(db_config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
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
                print(f"{cursor.rowcount} records inserted successfully.")
            

    else:
        print("没有获取到数据")
