'''
Author: wamgzwu
Date: 2024-12-30 15:45:40
LastEditors: wamgzwu wangzw26@outlook.com
LastEditTime: 2025-01-03 21:11:15
FilePath: \jfj_spider\dangdang_spider.py
Description: 党史-群书博览
'''
from DrissionPage import Chromium, ChromiumOptions
import DrissionPage
import pymysql
from datetime import datetime
import requests
import base64
import time
import logging
import random
import argparse
import re
import psycopg2
# from mysql.connector import Error 


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
        logging.FileHandler("dangshi_data_collection.log", mode='a', encoding='utf-8'),
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
    co.set_user_data_path('./data_dangshi')
    browser = Chromium(co)
    tab = browser.latest_tab
    # result_data = []
    adapter.info("START CRAWLING DATA FROM DANGSHI!")
    for page_num in range(1, 101):
        BASE_URL = "http://dangshi.people.com.cn/GB/146570/index{page_num}.html"
        url = BASE_URL.format(page_num=page_num)
        tab.get(url)
        tab.wait.doc_loaded()
        check_login(tab, adapter)
        # tab.scroll.to_bottom()
        result_div = tab.eles("x://div[@class='box']//div[@class='p1_content']//div[@class='p1_left']//div[@class='tw1 m15']")
        i = 0
        for div in result_div:
            result_data = []
            # 提取标题和详情链接
            try:
                title_element = div.ele("x://a/b")  # 定位标题
                title = title_element.text if title_element else "未找到标题"
                detail_url = div.ele("x://a").attr("href")  # 获取详情页链接
                print(f"标题: {title}, 详情页链接: {detail_url}")

                # 提取 ID 片段
                id_segment = "/".join(detail_url.split("/")[-3:-1])  # 提取ID片段
                id = id_segment.replace("/", "")  # 去掉斜杠
                print(f"记录ID: {id}")
            except Exception as e:
                print(f"无法获取标题或链接: {e}")
                continue
            print(id)
            # 判断数据是否存在于数据库中
            if select_data_from_database(f'dangshi_{id}', adapter):
                i += 1
                if mode == 1:  # 全量模式
                    adapter.info(f"第{page_num}页第{i}条数据开始更新!")
                    # 在此执行数据更新的逻辑
                else:
                    adapter.info(f"第{page_num}页第{i}条数据已存在, 跳过!")
                    continue  # 如果数据已经存在，则跳过当前项
            else:
                adapter.info(f"第{page_num}页第{i}条数据不存在, 开始采集!")
                
            # 进入详情页采集数据
            print(detail_url)
            new_tab = browser.new_tab(detail_url)
            time.sleep(random.randint(1, 3))
            new_tab.wait.doc_loaded()
            
            
         
            #image_element = new_tab.ele("x://div[@class='left fl']//img",timeout=20)

            image_element = new_tab.ele("x://div[@class='p1_left fl']//img",timeout=20)

            
                 
            image_src = image_element.attr('src')
            # 获取图片链接,并将其转为base64码
            img_base64 = img_to_base64(image_src,id)

            

            

            # 获取书的简介
            try:
                content_intro = new_tab.ele("x://div[@class='jianjie']").text
                # 去除多余的空格和换行符
                content_intro = content_intro.strip().replace("\u00a0", "")
            except Exception as e:
                content_intro = ""

            # 获取作者名字
            try:
                # 提取作者信息，假设作者信息在 class 为 'center fl' 下的 p 标签内
                author = new_tab.ele("x://div[@class='center fl']//p").text
                # 去除多余的空格和换行，只保留实际的作者名字
                author = author.strip().replace("\u00a0", "").split("　")[0]  # 提取名字部分
            except Exception as e:
                author = ""
            # 获取出版社名称
            try:
                # 提取包含出版社信息的文本
                publisher_info = new_tab.ele("x://div[@class='center fl']//p").text
    
                # 去除多余的空格和换行符
                publisher_info = publisher_info.strip().replace("\u00a0", "").replace("\n", "")
    
                # 通过分割符提取作者和出版社
                parts = publisher_info.split("　")  # 按分隔符 "　" 切分内容
    
                # 通常出版社会在第二段
                if len(parts) > 2:
                    publisher = parts[1].strip()  # 第二段为出版社信息
                else:
                    publisher = ""
    
            except Exception as e:
                publisher = ""  # 遇到异常返回空字符串

            print(f"出版社: {publisher}")




            # 获取出版年份
            try:
                # 提取包含出版年份的文本
                publication_info = new_tab.ele("x://div[@class='center fl']//p").text
                # 通过正则表达式提取年份信息
                import re
                match = re.search(r"(\d{4})年出版", publication_info)  # 匹配四位数字和"年出版"
    
                if match:
                    # 提取年份并格式化
                    publication_year = match.group(1)
                    # 将年份转换为完整日期格式 (yyyy-01-01 00:00:00)
                    publication_year = f'{publication_year}-01-01 00:00:00'
                else:
                    publication_year = ""  # 如果没有找到匹配的年份

            except Exception as e:
                publication_year = ""  # 出现异常时返回空字符串

            print(f"出版年份: {publication_year}")


            # 获取作者简介
            try:
                # 定位到包含作者简介的元素
                author_intro = new_tab.ele("x://div[@class='right fr']//p").text
            except Exception as e:
                author_intro = ""

            # 获取分类信息
            try:
                # 提取包含分类的文本
                category_info = new_tab.ele("x://div[@class='x_nav clearfix']").text
                # 去除多余的空格和换行
                category_info = category_info.strip().replace("\u00a0", "").replace("\n", "")
                # 按照 '>>' 分割，获取倒数第二个部分
                category_parts = category_info.split(" >> ")
                if len(category_parts) > 2:
                    # 保留倒数第二部分和后面的 '>>'，并拼接
                    subcategory = " >> ".join(category_parts[:-1]) + " >> " + category_parts[-1]
                else:
                    subcategory = category_info  # 如果没有足够多的部分，则保留全部分类信息
            except Exception as e:
                subcategory = ""


            
            try:
                # 获取精彩篇章的内容
                content_section = new_tab.ele("x://div[@class='p1_content']")

                # 提取每篇内容
                title1s = content_section.eles("x://b//font")
                contents = content_section.eles("x://div[@style='font-size: 12px;']")
                links = content_section.eles("x://p[@class='tr']//a")
                # 拼接结果
                final_content = ""
                for i in range(len(title1s)):
                    title1 = title1s[i].text
                    content = contents[i].text.strip()
                    link = links[i].attr("href")
                    final_content += f"标题: {title1}\n内容: {content}\n阅读链接: {link}\n------\n"
            except Exception as e:
                final_content=""


            # 获取目录部分的 URL
            try:
                # 寻找包含"目录"链接的<a>元素
                directory_link = new_tab.ele("x://div[@class='t03_2']//a[contains(text(), '目录')]")
                if directory_link:
                    # 获取目录页面的 URL
                    directory_url = directory_link.attr('href')
        
                    # 进入目录页面
                    new_tab.get(directory_url)
        
                    # 提取目录内容
                    try:
                        # 获取目录页面中的章节内容
                        content_section = new_tab.ele("x://div[@class='t2_content t2_text']//div[@class='text_show']")
                        chapters = content_section.text.strip().split('\n')  # 按行拆分章节内容
                        # 输出章节内容
                        catalogue = "\n".join([chapter.strip() for chapter in chapters if chapter.strip()])
                    except Exception as e:
                        catalogue = ""
                else:
                    catalogue = ""  # 如果没有找到"目录"链接
            except Exception as e:
                catalogue = ""



            # print(f"ISBN: {ISBN}, format: {formats}, paper: {paper}, package: {package}, category: {category}")
            
            data = {
                "id":id,
                "str_id": f'dangshi_{id}',  # 数据的唯一标识
                "title": title,  # 书名
                "author": author,  # 作者
                "author_intro": author_intro,  # 作者简介
                "publisher": publisher,  # 出版社
                "publish_time": publication_year,  # 出版年份
                "content_intro": content_intro,  # 图书简介
                "content_section": final_content,  # 精彩篇章内容（含标题、内容和链接）
                "image_url": image_src,  # 图片链接
                "base64_url": img_base64,  # 图片Base64编码
                "page_url": detail_url,  # 图书详情页链接
                "data_type": "党史",  # 数据来源
                "data_status": "1",  # 数据状态
                "book_type": "1",  # 图书类型
                "subcategory":subcategory, #分类
                "catalogue":catalogue,#目录
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
        # 建立数据库连接
        connection = psycopg2.connect(
            host="localhost",  # 主机名
            port=5432,  # PostgreSQL 默认端口
            user="postgres",  # 数据库用户名
            password="geek742028",  # 数据库密码
            dbname="postgres",  # 数据库名称
            options="-c client_encoding=UTF8"  # 确保使用 UTF-8 编码
        )
        print("成功连接到数据库")
        print(f"dataid: {dataid}")

        # 查询数据
        with connection.cursor() as cursor:
            select_query = "SELECT * FROM rs_correct_resources WHERE str_id = %s"
            cursor.execute(select_query, (dataid,))
            result = cursor.fetchone()

            if result:
                print("数据已存在，返回结果")
                return result
            else:
                print("数据不存在，开始采集")
                # 可以在这里调用采集函数
                return None

    except psycopg2.Error as e:
        print("数据库操作失败:", e)
        return None

    finally:
        # 关闭连接前检查状态
        if 'connection' in locals() and connection.closed == 0:
            connection.close()
            print("数据库连接已关闭")
# 全量采集时如果数据存在则对其进行更新, 目前只用更新价格和评论数

def get_connection():
    """统一管理数据库连接"""
    return psycopg2.connect(
        host="localhost",  # 主机名
        port=5432,         # PostgreSQL 默认端口
        user="postgres",   # 数据库用户名
        password="geek742028",  # 数据库密码
        dbname="postgres",  # 数据库名称
        options="-c client_encoding=UTF8"  # 确保使用 UTF-8 编码
    )

def update_data_to_database(id, price, evaluation_number, adapter):
    """更新数据到数据库"""
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
        adapter.error(f"更新数据失败: {e}")
    finally:
        if connection and connection.closed == 0:
            connection.close()

def save_data_to_database(result_data, adapter):
    """保存数据到数据库"""
    try:
        connection = get_connection()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_dict = {
            "id":result_data[0]["id"],
            "str_id": result_data[0]["str_id"],
            "title": result_data[0]["title"],
            "author": result_data[0]["author"],
            "author_intro": result_data[0]["author_intro"],
            "publish": result_data[0]["publisher"],
            "publish_time": result_data[0]["publish_time"],
            "content_intro": result_data[0]["content_intro"],
            "image_url": result_data[0]["image_url"],
            "base64_url": result_data[0]["base64_url"],
            "page_url": result_data[0]["page_url"],
            "data_type": result_data[0]["data_type"],
            "data_status": result_data[0]["data_status"],
            "create_time": current_time,
            "update_time": current_time,
            "deleted": 0,
            "book_type": result_data[0]["book_type"],
            "content_section": result_data[0]["content_section"],
            "subcategory": result_data[0]["subcategory"],
            "catalogue": result_data[0]["catalogue"]
        }
        with connection.cursor() as cursor:
            columns = ', '.join(data_dict.keys())
            placeholders = ', '.join(['%s'] * len(data_dict))
            sql_query = f"INSERT INTO rs_correct_resources ({columns}) VALUES ({placeholders})"
            cursor.execute(sql_query, list(data_dict.values()))
            connection.commit()
            adapter.info("数据已成功插入到表中")
    except Exception as e:
        adapter.error(f"插入数据失败: {e}")
    finally:
        if connection and connection.closed == 0:
            connection.close()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="党史网数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集")
    args = parser.parse_args()
    mode = args.m
    crawl_data(mode)