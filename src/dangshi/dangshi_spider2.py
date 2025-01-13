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
    for page_num in range(1, 16):
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
            try:
                response = requests.head(detail_url, timeout=10)  # 发送HEAD请求检查状态码
                if response.status_code in [404, 403]:
                    print(f"详情页链接无效，状态码: {response.status_code}. 跳过采集!")
                    continue  # 跳过无效的链接
            except requests.RequestException as e:
                print(f"检查详情页链接时出错: {e}. 跳过采集!")
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
            
            
            try:
                image_element = new_tab.ele("x://div[@class='t16']//img", timeout=20)

                #image_element = new_tab.ele("x://div[@class='p1_left fl']//img",timeout=20)

                
                    
                image_src = image_element.attr('src')
                # 获取图片链接,并将其转为base64码
                img_base64 = img_to_base64(image_src,id)
            except Exception as e:
                image_src=""
                img_base64=""

            

            try:
                # 获取书的简介
                intro_element = new_tab.ele("x://table[@class='t08']//tr[2]//td")  # 定位到包含简介的 <td>
                content_intro = intro_element.text  # 获取简介文本
                # 去除多余的空格和换行符
                content_intro = content_intro.strip().replace("\u00a0", "").replace("\n", "").replace("\r", "")
                print(f"书的简介: {content_intro}")
            except Exception as e:
                content_intro = ""
                print(f"无法获取书的简介: {e}")


           # 获取作者名字
            try:
                # 假设作者名字和简介在同一个 <td> 中
                author_info_element = new_tab.ele("x://td[@class='t10']")  # 定位到包含作者信息的 <td>
                author_info = author_info_element.text.strip()  # 提取并清理文本内容
                
                # 分割出作者名字和简介
                author_parts = author_info.split("，", 1)  # 按第一个逗号分割
                author = author_parts[0].strip()  # 提取作者名字
                author_intro = author_parts[1].strip() if len(author_parts) > 1 else ""  # 提取简介部分
                print(f"作者: {author}")
                print(f"作者简介: {author_intro}")
            except Exception as e:
                author = ""
                author_intro = ""
                print(f"无法获取作者信息: {e}")

            # 获取出版社名称
            try:
                # 定位包含出版社信息的 <td> 标签
                publisher_element = new_tab.ele("x://td[@class='t12']")  # 定位到包含出版社信息的元素
                publisher_info = publisher_element.text.strip()  # 提取并清理文本内容

                # 提取出版社名称，去除多余的前缀
                if "出版社：" in publisher_info:
                    publisher = publisher_info.split("出版社：")[-1].strip()  # 提取“出版社：”后的内容
                else:
                    publisher = publisher_info.strip()  # 如果没有明确前缀，直接取内容

                print(f"出版社: {publisher}")
            except Exception as e:
                publisher = ""
                print(f"无法获取出版社名称: {e}")





            # 获取出版年份
            try:
                # 提取包含出版年份的文本
                publication_info = new_tab.ele("x://div[@class='center fl']//p").text
                
                # 使用正则表达式提取年份
                import re
                match = re.search(r"(\d{4})年出版", publication_info)  # 匹配四位数字和"年出版"
                
                if match:
                    # 提取年份
                    publication_year = match.group(1)
                    
                    # 将年份转换为完整的日期格式 (YYYY-MM-DD)
                    publication_date = f"{publication_year}-01-01"  # 默认设置为 1 月 1 日
                else:
                    publication_date = ""  # 如果未找到匹配的年份，设置为空字符串

            except Exception as e:
                publication_date = ""  # 出现异常时返回空字符串

            print(f"出版日期: {publication_date}")




            try:
                # 提取包含分类的文本
                category_info = new_tab.ele("x://td[@class='t01']").text  # 修正 XPath 定位分类信息
                # 去除多余的空格和换行
                category_info = category_info.strip().replace("\u00a0", "").replace("\n", "")
                # 按照 '>>' 分割，获取分类层级
                category_parts = category_info.split(" >> ")
                if len(category_parts) > 1:
                    # 保留倒数第二部分和最后一部分
                    subcategory = f"{category_parts[-2]} >> {category_parts[-1]}"
                else:
                    subcategory = category_info  # 如果分类层级不足两级，则直接保留全部信息
            except Exception as e:
                subcategory = ""



            
            try:
                # 获取精彩篇章的内容
                content_sections = new_tab.eles("x://table[@width='96%']")  # 定位每个章节的 table 块

                # 拼接结果
                final_content = ""
                for section in content_sections:
                    # 提取标题
                    title_elem = section.ele("x://tr//td[@class='t03']/a")
                    title1 = title_elem.text
                    link = title_elem.attr("href")

                    # 提取内容
                    content_elem = section.ele("x://tr//td[@class='t04']")
                    content = content_elem.text.strip()

                    # 组合内容
                    final_content += f"标题: {title1}\n内容: {content}\n阅读链接: {link}\n------\n"

            except Exception as e:
                final_content = ""



            try:
                # 定位包含“目录”文本的 <a> 元素
                directory_link = new_tab.ele("x://a[contains(text(), '目录')]")  # 使用 XPath 定位
                print(directory_link)
                if directory_link:
                    # 获取目录页面的 URL
                    directory_url = directory_link.attr('href')
                    print(f"目录页面 URL: {directory_url}")

                    # 进入目录页面
                    new_tab.get(directory_url)

                    # 提取目录内容
                    try:
                        # 定位目录页面的主要内容区域
                        content_section = new_tab.ele("x://div[contains(@class, 't2_content')]//div[contains(@class, 'text_show')]")
                        if content_section:
                            # 获取章节文本并按行分割，去除空行
                            chapters = content_section.text.strip().split('\n')
                            # 整理为完整的目录内容
                            catalogue = "\n".join([chapter.strip() for chapter in chapters if chapter.strip()])
                            print(f"目录内容:\n{catalogue}")
                        else:
                            print("目录内容区域未找到")
                            catalogue = ""
                    except Exception as e:
                        print(f"提取目录内容失败: {e}")
                        catalogue = ""
                else:
                    print("未找到目录链接")
                    catalogue = ""
            except Exception as e:
                print(f"获取目录 URL 时发生错误: {e}")
                catalogue = ""


            



            # print(f"ISBN: {ISBN}, format: {formats}, paper: {paper}, package: {package}, category: {category}")
            
            data = {
                "str_id": f'dangshi_{id}',  # 数据的唯一标识
                "title": title,  # 书名
                "author": author,  # 作者
                "author_intro": author_intro,  # 作者简介
                "publisher": publisher,  # 出版社
                "publish_time": publication_date,  # 出版年份
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
        # 连接到MySQL数据库
        connection = pymysql.connect(
            host="localhost",  # 仅填写主机名
            port=3306,  # 指定端口
            user="root",
            password="geek742028",
            database="reslib",
            charset="utf8mb4"  # 设置字符集为utf8mb4
        )
        if connection.open:
            print("成功连接到数据库")
            print(f"dataid: {dataid}")
            dataid = str(dataid)
            # 查询数据
            cursor = connection.cursor()
            select_query = f"SELECT * FROM rs_correct_resources WHERE str_id='{dataid}'"
            cursor.execute(select_query)
            result = cursor.fetchone()
            if result:
                
                connection.close()
                return result
            else:
                print("数据不存在，开始采集")
            
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
            password="geek742028",
            database="reslib",
            charset="utf8mb4"  # 设置字符集为utf8mb4
        )
        if connection.open:
            # print("成功连接到数据库")
            # 查询所有需要更新的数据
            cursor = connection.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if evaluation_number is None:
                update_query = f"UPDATE rs_correct_resources SET price={price}, update_time='{current_time}' WHERE str_id='{id}'"
            else:
                update_query = f"UPDATE rs_correct_resources SET price={price}, evaluation_number={evaluation_number}, update_time='{current_time}' WHERE str_id='{id}'"
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
            password="geek742028",
            database="reslib",
            charset="utf8mb4"  # 设置字符集为utf8mb4
        )
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_dict = {
            
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
            "content_section":result_data[0]["content_section"],
            "subcategory":result_data[0]["subcategory"],
            "catalogue":result_data[0]["catalogue"]
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
            # adapter.info(f'sql语句:{all_sql_str}')
            cursor.execute(all_sql_str,value_list)
            connection.commit()
            adapter.info(f"数据已成功插入到表中")

    except Exception as e:
       # print("错误:", e)
        adapter.error(e)

    finally:
    # 确保 cursor 和 connection 都被正确关闭
        if cursor:
            cursor.close()  # 只有在 cursor 已经成功创建后才会关闭
        if connection.open:
            connection.close()  # 关闭数据库连接
        print("数据库连接已关闭")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="党史网数据采集")
    parser.add_argument("-m", type=int, required=True, default=0, help="选择采集模式, 0为增量采集, 1为全量采集")
    args = parser.parse_args()
    mode = args.m
    crawl_data(mode)