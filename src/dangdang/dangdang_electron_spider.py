import requests as r
import time
import random
import logging
from DrissionPage import Chromium
import base64
import re
from datetime import datetime
import pymysql
import argparse

BASE_URL = "https://e.dangdang.com/media/api.go?action=searchMedia&enable_f=1&promotionType=1&keyword=%E5%86%9B%E4%BA%8B&deviceSerialNo=html5&macAddr=html5&channelType=html5&returnType=json&channelId=70000&clientVersionNo=6.8.0&platformSource=DDDS-P&fromPlatform=106&deviceType=pconline&stype=media&mediaTypes=1%2C2"
DETAIL_URL = "https://e.dangdang.com/media/api.go?action=getMedia&deviceSerialNo=html5&macAddr=html5&channelType=html5&returnType=json&channelId=70000&clientVersionNo=6.8.0&platformSource=DDDS-P&fromPlatform=106&deviceType=pconline&refAction=browse&promotionType=1"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
}
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
        logging.FileHandler("dangdang_el_data_collection.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# 创建 logger 对象
logger = logging.getLogger(__name__)

def check_login(tab, adapter):
    if len(tab.eles("x://span[@id='nickname']//a[@dd_name='登录']", timeout = 1)) != 0: #需要登录
        time.sleep(2)
        tab.ele("x://span[@id='nickname']//a[@dd_name='登录']").click()
        while True:
            if len(tab.eles("x://span[@id='nickname']//a[@target='_self']", timeout=1)) != 0:
                adapter.info("登录成功!")
                break
            time.sleep(2)
            adapter.warning("需要人工辅助登录!")
    else:
        adapter.info("检测到已登录, 开始采集数据")

def img_to_base64(img_url):
    response = r.get(img_url)
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
    
def extract_price(price_string):
    # 正则表达式：匹配以"¥"开头的价格，捕获价格数字（包括小数）
    match = re.search(r'(\d+(\.\d+)?)', price_string)
    
    if match:
        # 提取匹配到的数字部分并转换为浮动数
        return match.group(1)
    else:
        return None  # 如果没有找到有效价格，返回 None
def crawl_data(mode: int):
    if mode == 0:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Increment Mode]'})
    else:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Full Mode]'})
    adapter.info("开始采集当当网电子书数据...")
    # 检测登录
    browser = Chromium()
    tab = browser.latest_tab
    tab.get("https://e.dangdang.com/newsearchresult_page.html?keyword=%E5%86%9B%E4%BA%8B")
    tab.wait.doc_loaded()
    check_login(tab, adapter)
    # 登录成功后获取该页面的token
    cookies = tab.cookies()
    for cookie in cookies:
        if cookie['name'] == 'sessionID':
            token = cookie['value']
        if cookie['name'] == '__permanent_id':
            permanentId = cookie['value']
    adapter.info(f"获取到token: {token}")
    adapter.info(f"获取到permanentId: {permanentId}")
    time.sleep(2)
    # 获取总页数
    page_num_str = tab.ele("x://div[@id='leftWrap']//div[@id='pageWrap']//span[@id='countNum']", timeout=1).text
    page_num = int(re.search(r'\d+', page_num_str).group())
    exist_ids = load_existing_ids()
    adapter.info(f"总共 {page_num} 页数据...")
    for page in range(page_num):
        adapter.info(f"开始采集第 {page+1} 页数据...")
        book_list = tab.ele("#conditionList").eles("x://div[@class='list book_list_wrap clearfix']", timeout=1)
        count = 0
        for book in book_list:
            result_data = []
            book_info = book.ele("x://div[@class='detail_con']", timeout=1)
            detail_url = book_info.ele("x://p[@class='title']//a", timeout=1).attr("href")
            id = detail_url.split("/")[-1].split(".")[0]
            str_id = f"dangdang_el_{id}"
            if str_id in exist_ids:
                count += 1
                if mode == 1:
                    # 全量模式更新价钱
                    price_str = book_info.ele("x://p[@class='price']", timeout=1).text
                    price = extract_price(price_str)
                    update_data_to_database(str_id, price, adapter)
                else:
                    adapter.info(f"第{page+1}页第{count}条数据已存在!")
                continue
            # 把str_id加到集合中
            exist_ids.add(str_id)
            book_pic_href = book.ele("x://a//img[@class='book_pic']", timeout=1).attr("src")
            image_base64 = img_to_base64(book_pic_href) # 图片转base64
            title = book_info.ele("x://p[@class='title']", timeout=1).text
            author = book_info.ele("x://p[@class='author']", timeout=1).text
            price_str = book_info.ele("x://p[@class='price']", timeout=1).text
            price = extract_price(price_str)
            new_tab = browser.new_tab(detail_url)
            new_tab.wait.doc_loaded()
            # 获取详情页的数据
            # 只在页面采集目录内容, 其他的字段均通过数据包获取
            catalog = new_tab.ele("#catalogModule", timeout=1)
            catalog.ele("#isshow").click() # 点击展开目录
            catalog_content_list = catalog.ele("#catalog_title", timeout=1).eles("x://p")
            # 获取目录的内容
            catalog_content = ''
            for catalog_item in catalog_content_list:
                catalog_content += catalog_item.text + "\n"
            # 请求详情数据
            detail_response = r.get(
                url=f"{DETAIL_URL}&permanentId={permanentId}&token={token}&saleId={id}",
                headers=HEADERS,
                timeout=5,
                verify=True
            )
            if detail_response.status_code == 200:
                count += 1
                adapter.info(f"第 {page+1} 页, 第 {count} 本数据详情请求成功!")
                detail_data = detail_response.json()['data']['mediaSale']['mediaList'][0]
                # 选取所需字段数据
                title = detail_data['title']
                author = detail_data['authorPenname']
                price = detail_data['salePrice']
                ISBN = detail_data['isbn']
                comment_num = detail_data['commentNumber']
                word_count = detail_data['wordCnt']
                category = detail_data['categoryForSingleProductPageDesc']
                editor_recommendations = detail_data['editorRecommend']
                content_intro = detail_data['descs']
                publisher = detail_data['publisher']
                publish_time = datetime.fromtimestamp(int(detail_data['publishDate'])/1000)
                data = {
                    "str_id": str_id,
                    "title": title,
                    "author": author,
                    "publisher": publisher,
                    "publish_time": publish_time,
                    "catalogue": catalog_content,
                    "content_intro": content_intro,
                    "ISBN": ISBN,
                    "subcategory": category,
                    "price": float(price),
                    "evaluation_number": comment_num,
                    "editor_recommendations": editor_recommendations,
                    "words": word_count,
                    "image_url": book_pic_href,
                    "image_base64": image_base64,
                    "page_url": detail_url,
                    "data_type": "当当",
                    "data_status": "1",
                    "book_type": "2"
                }
                result_data.append(data)
                save_data_to_database(result_data, adapter)
            browser.close_tabs(new_tab)
            time.sleep(random.randint(4,8))
        # 翻页
        if page !=  page_num - 1:
            tab.ele("x://div[@id='pageWrap']//p[@class='nextPage']", timeout=1).click()
            time.sleep(random.randint(1,2)) # 翻页之后稍微等一下, 防止数据还没加载就已经开始采集, 那样采集的就是上一页的




    # for start in range(page_num):
    #     adapter.info(f"采集第 {start+1} 页数据, 开始请求...")
    #     response = r.get(
    #                 url=f"{BASE_URL}&permanentId={permanentId}&token={token}&start={start*10}&end={(start+1)*10}",
    #                 headers=HEADERS,
    #                 timeout=5,
    #                 verify=True
    #             )
    #     if response.status_code == 200:
    #         adapter.info(f"第 {start+1} 页数据请求成功!")
    #         data = response.json()
    #         count = 0
    #         for item in data['data']['searchMediaPaperList']:
    #             count += 1
    #             # 请求详情数据
    #             detail_response = r.get(
    #                 url=f"{DETAIL_URL}&permanentId={permanentId}&token={token}&saleId={item['saleId']}",
    #                 headers=HEADERS,
    #                 timeout=5,
    #                 verify=True
    #             )
    #             if detail_response.status_code == 200:
    #                 adapter.info(f"第 {start+1} 页, 第 {count} 本数据详情请求成功!")
    #                 detail_data = detail_response.json()['data']['mediaSale']['mediaList'][0]
    #                 # 选取所需字段数据
    #                 id = f"dangdang_el_{detail_data['saleId']}" # 当当电子书格式
    #                 title = detail_data['title']
    #                 author = detail_data['authorPenname']
    #                 price = detail_data['salePrice']
    #                 ISBN = detail_data['isbn']
    #                 comment_num = detail_data['commentNumber']
    #                 word_count = detail_data['wordCnt']
    #                 # 书籍封面
    #                 img_url = detail_data['coverPic']
    #                 image_base64 = img_to_base64(img_url) # 图片转base64


    #             time.sleep(random.randint(4,8))
    #     else:
    #         adapter.error(f"第 {start+1} 页数据请求失败!")
    #         continue
def load_existing_ids():
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
            cursor = connection.cursor()
            select_all_query = f"SELECT DISTINCT str_id FROM rs_correct_resources"
            cursor.execute(select_all_query)
            connection.commit()
            existing_ids = set(row[0] for row in cursor.fetchall())
            return existing_ids
    except Exception as e:
        pass
    finally:
        if connection.open:
            cursor.close()
            connection.close()



# 数据更新
def update_data_to_database(id, price, adapter):
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
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            update_query = f"UPDATE rs_correct_resources SET price={price}, update_time='{current_time}' WHERE str_id='{id}'"
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
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_dict = {
            "str_id": result_data[0]["str_id"],
            "title": result_data[0]["title"],
            "author": result_data[0]["author"],
            "publish": result_data[0]["publisher"],
            "publish_time": result_data[0]["publish_time"],
            "catalogue": result_data[0]["catalogue"],
            "content_intro": result_data[0]["content_intro"],
            "ISBN": result_data[0]["ISBN"],
            "subcategory": result_data[0]["subcategory"],
            "price": result_data[0]["price"],
            "evaluation_number": result_data[0]["evaluation_number"],
            "editor_recommendations": result_data[0]["editor_recommendations"],
            "words": result_data[0]["words"],
            "image_url": result_data[0]["image_url"],
            "image_base64": result_data[0]["image_base64"],
            "page_url": result_data[0]["page_url"],
            "data_type": result_data[0]["data_type"],
            "data_status": result_data[0]["data_status"],
            "create_time": current_time,
            "update_time": current_time,
            "deleted": 0,
            "book_type": result_data[0]["book_type"]
        }
        if connection.open:
            cursor = connection.cursor()
            str_sql_head = 'INSERT INTO rs_correct_resources'
            str_sql_middle = ""
            str_sql_value = ""
            value_list = []
            for key,value in data_dict.items():
                if value:
                    str_sql_middle = str_sql_middle + key + ","
                    str_sql_value += "%s,"
                    value_list.append(value)
            #去除末尾,
            str_sql_middle = str_sql_middle.rstrip(",")
            str_sql_value = str_sql_value.rstrip(",")
            str_sql_middle = f"({str_sql_middle})"
            str_sql_value = f"VALUES ({str_sql_value})"
            all_sql_str = f"{str_sql_head} {str_sql_middle} {str_sql_value}"
            cursor.execute(all_sql_str,value_list)
            connection.commit()
            adapter.info(f"数据已成功插入到表中")

    except Exception as e:
       # print("错误:", e)
        adapter.error(e)

    finally:
        if connection.open:
            cursor.close()
            connection.close()
            # print("数据库连接已关闭")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="当当网电子书数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集")
    args = parser.parse_args()
    mode = args.m
    crawl_data(mode)