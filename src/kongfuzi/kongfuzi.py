'''
Author: wamgzwu
Date: 2024-12-30 15:45:40
LastEditors: wamgzwu wangzw26@outlook.com
LastEditTime: 2025-01-03 21:11:15
FilePath: \jfj_spider\dangdang_spider.py
Description: 当当采集
'''
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

# 采集需要全量采集 增量采集
# 全量采集重新爬取整个网站
# 增量采集只爬取最新的数据
# 首先第一步 获取当前的所有child_urls
# 依次爬取
# 后面根据是否新增数据来判断，新增数据

# 第一个函数 获取child_urls
# 第二个函数 采集数据
# 第三个函数 更新数据,判断新增数据
# 第四个函数 更新child_urls

# 写入数据


# 检测是否登录, 如果没有则提示用户请登录(这里登录需要用户手动辅助)
def is_logged_in(tab):
    # 获取指定的 Cookie（假设登录状态由 'session_id' 表示）
    cookies = tab.cookies().as_json()  # 替换为实际的 Cookie 名称
    for cookie in cookies:
        if "reciever_area" in cookie:
            return True
    return False


def check_login(tab):
    login_button_xpath = "@@tag()=input@@@value()=登录"

    # 检查是否需要登录
    login_elements = tab.eles(login_button_xpath, timeout=1)
    if login_elements:
        print("需要登录，正在等待用户操作...")
        sleep(2)  # 等待页面稳定
        login_elements[0].click()

        # 等待用户完成登录
        while not is_logged_in(tab):
            sleep(2)  # 等待一段时间后重新检查

        print("登录成功！")
    else:
        print("检测到已登录, 开始采集数据")


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

def get_child_urls(base_url, page, output="./output/", mode=0):
    # 检查输出目录
    if not os.path.exists(output):
        os.makedirs(output)
    # 定义文件路径
    all_file_path = os.path.join(output, "child_all.txt")
    old_file_path = os.path.join(output, "child_old.txt")
    new_file_path = os.path.join(output, "child_new.txt")

    if mode == 0:
        # 全量爬取
        print("第一次全量爬取")
        child_urls = []

        print("开始爬取所有页...")
        last_page = 100  # 假设总页数为100，可动态获取
        with open(all_file_path, "w") as out_file:
            for current_page in tqdm(range(1, last_page + 1), desc="爬取字网页进度"):
                print(f"正在爬取第 {current_page} 页的子网页...")
                url = base_url + str(current_page)
                print(url)
                # 发送 POST 请求
                page.get(url)
                page_json = page.json
                # 检查返回数据
                if not page_json or "data" not in page_json or "itemResponse" not in page_json["data"] or not page_json["data"]["itemResponse"]["list"]:
                    print(f"第 {current_page} 页无数据，爬取结束。")
                    break
                # 提取 mids 并生成子网页链接
                mids = [item['mid'] for item in page_json['data']['itemResponse']['list']]
                for mid in mids:
                    child_url = f"https://item.kongfz.com/book/{mid}.html"
                    child_urls.append(child_url)
                    out_file.write(child_url + "\n")

        # 备份全量爬取结果为旧文件
        #将all_file_path重命名为old_file_path
        if os.path.exists(all_file_path):
            os.rename(all_file_path, old_file_path)

        print("爬取完成!")
        print(f"共爬取 {len(child_urls)} 个子网页，结果保存在 {all_file_path}")

    elif mode == 1:
        # 增量爬取
        print("增量爬取模式")
        # 读取上次的结果
        if not os.path.exists(old_file_path):
            print("未找到旧文件，无法进行增量爬取，请先执行全量爬取！")
            return []
        # 读取旧数据
        with open(old_file_path, "r") as f:
            child_old_urls = set(line.strip() for line in f.readlines())
        print(f"读取到 {len(child_old_urls)} 条旧数据")
        child_urls = []
        current_page = 1
        success = True
        # 开始增量爬取
        with open(new_file_path, "w") as out_file:
            while success:
                try:
                    print(f"正在爬取第 {current_page} 页的子网页...")
                    url = base_url + str(current_page)
                    print(url)

                    # 发送 POST 请求
                    page.get(url)
                    page_json = page.json

                    # 检查返回数据
                    if not page_json or "data" not in page_json or "itemResponse" not in page_json["data"] or not page_json["data"]["itemResponse"]["list"]:
                        print(f"第 {current_page} 页无数据，爬取结束。")
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
                    print(f"爬取第 {current_page} 页时出现错误: {e}")
                    success = False
            
        # 将增量爬取结果写入新文件
        with open(all_file_path, "w") as all_file:
            all_urls = child_old_urls.union(child_urls)
            for url in all_urls:
                all_file.write(url + "\n")
        if len(child_urls) == 0:    
            print("增量爬取无数据，爬取结束。")
            return []
        print("增量爬取完成!")
        print(f"新增 {len(child_urls)} 条子网页，结果保存在 {new_file_path}")

    else:
        # 更新数据库
        print("更新数据库模式")
        with open(all_file_path, "r") as f:
            child_urls = [line.strip() for line in f.readlines()]




    return child_urls



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
def worker(task_queue, result_queue, failure_queue,cookies,port):
    """
    爬取工作线程。
    """


    so = SessionOptions()
    page = SessionPage(session_or_options=so)  # 用该配置创建页面对象


    while not task_queue.empty():
        url = task_queue.get()
        retry_count = 0
        MAX_RETRIES = 10
        while retry_count <= MAX_RETRIES:
            try:
                page.get(url=url,cookies=cookies)
                data = get_elems_multithreaded(url, page)
                result_queue.put(data)  # 将结果加入结果队列
                print(f"成功采集数据: {url}")
                break
            except Exception as e:
                retry_count += 1
                print(f"Error processing {url}: {e}, retry {retry_count}")


                if retry_count > MAX_RETRIES:
                    print(f"Failed after {MAX_RETRIES} retries: {url}")
                    failure_queue.put(url)
                        # 计算延迟时间并等待
                delay = exponential_backoff(retry_count)
                print(f"重试第 {retry_count} 次，等待 {delay:.2f} 秒后重试...")
                time.sleep(delay)
        task_queue.task_done()


def main(mode=0):
    #参数
    #代理
    #base_url
    #output_path





    #按照爬取模式获取child_urls
    if mode==0:
        #全量爬取
        child_urls = get_child_urls(base_url,page,output)
        pass
    elif mode==1:
        #增量爬取
        pass
    else:
        #更新数据库
        pass





    output = "./output/"
    if not os.path.exists(output):
        os.makedirs(output)

    if mode == 0:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Increment Mode]'})
    else:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Full Mode]'})
    
    so = SessionOptions()
    page = SessionPage(session_or_options=so)  # 用该配置创建页面对象

    # 第一步获取child_urls
    base_url = "https://search.kongfz.com/pc-gw/search-web/client/pc/bookLib/category/list?redirectFrom=booklib_category&catId=24&actionPath=catId&page="
    get_child_urls(base_url,page,output)

    #获取登录信息
    browser = Chromium()
    tab = browser.latest_tab
    tab.get('https://item.kongfz.com/book/41478539.html')
    check_login(tab)
    cookies = tab.cookies().as_json()
    print(f"登录信息为{cookies}")
    browser.close_tabs(tab)


    # 初始化队列
    task_queue = queue.Queue()
    result_queue = queue.Queue()
    failure_queue = queue.Queue()

    child_urls = child_urls[:10]
    for url in tqdm(child_urls):
        task_queue.put(url)

    # 创建并启动线程
    thread_count = 10  # 根据需要调整线程数量
    threads = []
    base_port = 9333  # 起始端口

    for i in range(thread_count):
        port = base_port + i  # 为每个线程分配一个唯一的端口
        t = threading.Thread(target=worker, args=(task_queue, result_queue, failure_queue, port,cookies))
        t.start()
        threads.append(t)

    # 等待所有任务完成
    for t in threads:
        t.join()

    # 写入结果
    with open(outfiles, 'w', encoding='utf-8') as file:
        while not result_queue.empty():
            data = result_queue.get()
            file.write('<REC>\n')
            for key, value in data.items():
                file.write(f"<{key}>={value}\n")
            file.write('<REC>\n')

    # 写入失败记录
    with open("falure_crdcn.txt", 'w') as file:
        while not failure_queue.empty():
            file.write(failure_queue.get() + '\n')

if __name__ == "__main__":


    parser = argparse.ArgumentParser(description="当当网数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集,2为更新数据库")
    args = parser.parse_args()
    mode = args.m
    main(mode)



