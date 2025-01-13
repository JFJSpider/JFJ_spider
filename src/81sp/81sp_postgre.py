import argparse
import re
import time
from datetime import datetime
from decimal import Decimal,ROUND_HALF_UP
from idlelib.replace import replace

import psycopg2
from DrissionPage import Chromium,ChromiumOptions
import requests
import base64
import mysql.connector
import logging
import requests


def download_video(url, str_id):
    # 发送 GET 请求获取视频文件
    response = requests.get(url, stream=True)

    #拼接视频保存地址
    save_path = f"./videos/{str_id}.mp4"
    # 检查响应状态
    if response.status_code == 200:
        # 打开文件并写入内容
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return True

    return False


def download_audio(url, str_id):
    # 发送 GET 请求获取视频文件
    response = requests.get(url, stream=True)

    #拼接视频保存地址
    save_path = f"./audios/{str_id}.mp3"
    # 检查响应状态
    if response.status_code == 200:
        # 打开文件并写入内容
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return True

    return False

#id:rmzxb_20240911_67456
class ZrclData:
    def __init__(self):
        self.str_id = None                  #id
        self.url = None                     #详情页url
        self.title = None                   #标题
        self.zuo_zhe = None                 #作者
        self.ze_bian = None                 #责编
        self.bian_ji = None                 #编辑
        self.lai_yuan = None                #来源
        self.shi_jian = None                #时间
        self.fen_lei = None                 #分类
        self.shi_pin_url = None             #视频url
        self.bian_shen = None               #编审

    def to_stander(self):
        """
        将 ZrclDataType 类实例转换为 数据库 格式。
        """
        #时间格式化
        #印刷时间，出版时间，首版时间

        formats = [
            "%Y年%m月%d日%H:%M",
            "%Y-%m-%d %H:%M:%S"# 包含完整日期和时间
        ]

        if self.shi_jian is not None:
            tmp_parser_time_flag = 1
            for fmt in formats:
                try:
                    self.shi_jian = datetime.strptime(self.shi_jian, fmt)
                    tmp_parser_time_flag = 0
                    break
                except ValueError as e:
                    continue
            if tmp_parser_time_flag:
                raise ValueError(f"Date string format is not recognized: {self.shi_jian}")

        if self.ze_bian is not None:
            self.ze_bian = self.ze_bian.replace("(责编：","").replace(")","")


def is_duplicate(str_id):
    #这里是判断给定的id，书名，ISBN是否已经在数据库中存在
    connection = psycopg2.connect(
        host="10.101.221.240",  # 数据库主机地址
        port="54321",
        user="reslib",  # 用户名
        password="1qaz3edc123!@#",  # 密码
        dbname="reslib"  # 数据库名称
    )
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM tb_resource WHERE str_id='{str_id}';")
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        title_index = columns.index('fn_title')
        if rows:
            #如果不是空
            row_now = rows[0]
            return True,row_now[title_index]

        else:
            #如果是空
            return False,None
    finally:
        cursor.close()
        connection.close()


def save_in_db(need_save:ZrclData):
    #将当前的数据保存进入DB
    connection = psycopg2.connect(
        host="10.101.221.240",  # 数据库主机地址
        port="54321",
        user="reslib",  # 用户名
        password="1qaz3edc123!@#",  # 密码
        dbname="reslib"  # 数据库名称
    )
    connection.autocommit = False  # 非自动提交
    cursor = connection.cursor()
    if "mp3" in need_save.shi_pin_url:
        type_temp = "音频"
    else:
        type_temp = "视频"
    data_dict = {
        "str_id": need_save.str_id,
        "from_cite":"八一视频",
        "category":need_save.fen_lei,
        "url":need_save.url,
        "fn_title":need_save.title,
        "fn_editor":need_save.bian_ji,
        "chief_editor":need_save.ze_bian,
        "create_time":datetime.now(),
        "update_time":datetime.now(),
        "fn_type":type_temp,
        "fn_source":need_save.lai_yuan,
        "video_url":need_save.shi_pin_url,
        "fn_pubtime":need_save.shi_jian,
        "fn_author":need_save.zuo_zhe,
        "editorial_reviewer":need_save.bian_shen,
    }
    str_sql_head = 'INSERT INTO tb_resource'
    str_sql_middle = ""
    str_sql_value = ""
    value_list = []
    for key,value in data_dict.items():
        if value is not None:
            str_sql_middle +=  "\"" + key+ "\""+ ","
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
        logging.FileHandler("81sp.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def main():
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description="81视频采集器")
    parser.add_argument("-m", type=int, default=0 , help="选择采集模式, 0为增量采集，默认为0")
    parser.add_argument("-d", type=int, default=50, help="增量模式下该参数有用，表示最大重复数，重复多少条记录后停止，默认为50")
    args = parser.parse_args()
    mode = args.m    #0表示增量模式，1为全量更新
    max_duplicate_time = args.d
    if mode == 0:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Increment Mode]'})
    else:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Full Mode]'})

    if mode == 0:
        adapter.info(f"81视频信息采集-增量模式-启动")
    if mode == 1:
        adapter.info(f"81视频信息采集-全量模式-启动")


    count_all = 0
    scan_num = 0
    update_num = 0
    co = ChromiumOptions()
    #co.headless(True)
    #co.set_load_mode('eager')
    browser = Chromium(co)
    # browser = Chromium()
    main_url = {"http://tv.81.cn/jszqy/index.html":"军事最前沿"}
    list_count = -1
    for each_url,each_value in main_url.items():
        duplicate_time = 0  # 连续的重复记录数
        list_count += 1
        adapter.info(f"列表{list_count+1}-八一视频-{each_value} 开始爬取")
        if duplicate_time >= max_duplicate_time:
            # 如果重复次数大于等于50 则退出该列表
            adapter.warning(f"连续重复数大于等于{max_duplicate_time}，列表{list_count+1}增量采集结束")
            continue
        #打开当前列表
        list_tab = browser.new_tab(each_url)
        base_url = each_url[:each_url.rfind('/')]+"/"
        if list_count == 0:
            #获取最大页码
            max_page_num = int(list_tab.ele(".more-page").child("下一页").prev().text)
            page_next = 1

            while page_next <= max_page_num:
                adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页开始采集")
                if duplicate_time >= max_duplicate_time:
                    #如果重复次数大于等于50 则退出
                    break

                #每个循环是一页
                #首先是进入正确的页码判断
                while True:
                    real_page_now = int(list_tab.ele(".more-page").child(".page").text)
                    if real_page_now == page_next:
                        adapter.info(f"进入了正确的页码：{page_next}")
                        break
                    adapter.info(f"调整进入正确页码中...")
                    if page_next == 1:
                        list_tab.get(f"{base_url}/index.html")
                    else:
                        list_tab.get(f"{base_url}/index_{page_next}.html")
                    time.sleep(1)


                #这里进入了正确的页码，page_next相当于page_now
                lis_all_video = list_tab.ele(".row").children("t=li")
                list_all_video_li = []
                for li_each_video in lis_all_video:
                    hrefs_all_video_for_one_li = li_each_video.ele("t=a").attr("href")
                    list_all_video_li.append(hrefs_all_video_for_one_li)

                #每一页获取一次分类
                fen_lei_as = list_tab.ele(".:breadcrumb").children("t=a")
                fen_lei_text = ""
                for fen_lei_a in fen_lei_as:
                    fen_lei_text += fen_lei_a.text + ">>"
                fen_lei_text = fen_lei_text.rstrip(">>")
                inpage_count = 1
                for href_each_video in list_all_video_li:
                    if duplicate_time >= max_duplicate_time:
                        break

                    one_element_start_time = time.time()
                    adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个开始采集")
                    id_now = href_each_video.split("/")[-1].replace(".html","")
                    list_now = href_each_video.split("/")[-2]
                    id_now = f"81sp_{list_now}_{id_now}"
                    duplicate_flag,video_title = is_duplicate(id_now)
                    if duplicate_flag:
                        #如果重复了
                        if mode == 0:
                            #如果是增量模式，那就跳过这一条，并且连续重复数+1
                            adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 重复并跳过，已连续重复{duplicate_time+1}次")
                            scan_num += 1
                            duplicate_time += 1
                            inpage_count += 1
                            continue
                        elif mode == 1:
                            #如果是全量模式，那就没啥可更新的好像
                            #全量模式那就扫描一遍，重复的就跳过
                            adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 重复并跳过")
                            scan_num += 1
                            inpage_count += 1
                            continue

                        else:
                            #暂时不支持的模式
                            raise Exception("暂不支持的模式参数，请检查参数设置")

                    else:
                        #如果没重复
                        duplicate_time = 0
                    one_element = ZrclData()

                    #打开一个循环的页面
                    video_tab = browser.new_tab(href_each_video)


                    #url
                    one_element.url = video_tab.url


                    #分类
                    one_element.fen_lei = fen_lei_text


                    #id
                    article_id = one_element.url.split("/")[-1].replace(".html","")
                    article_id = one_element.url.split("/")[-2]+"_"+article_id
                    one_element.str_id = f"81sp_{article_id}"

                    #title
                    one_element.title = video_tab.ele("t=h2").text.strip()

                    #下面详情部分
                    p_all_info = video_tab.ele(".video-info-right").children("t=p")
                    for p_each_info in p_all_info:
                        if "来源：" in p_each_info.text:
                            one_element.lai_yuan = p_each_info.text.replace("来源：","").strip()
                            continue

                        if "作者：" in p_each_info.text:
                            one_element.zuo_zhe = p_each_info.text.replace("作者：","").strip()
                            continue

                        if "编辑：" in p_each_info.text:
                            one_element.bian_ji = p_each_info.text.replace("编辑：","").strip()
                            continue

                        if "责编：" in p_each_info.text:
                            one_element.ze_bian = p_each_info.text.replace("责编：","").strip()
                            continue

                        if "发布：" in p_each_info.text:
                            one_element.shi_jian = p_each_info.text.replace("发布：","").strip()
                            continue

                        if "编审：" in p_each_info.text:
                            one_element.bian_shen = p_each_info.text.replace("编审：", "").strip()
                            continue

                        adapter.critical(
                            f"列表{list_count + 1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 编著出现了未知字段！str_id={one_element.str_id} url={one_element.url}")
                        raise Exception(f"列表{list_count + 1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 编著出现了未知字段！str_id={one_element.str_id} url={one_element.url}")


                    #视频下载
                    href_video_mp4 = video_tab.ele("#download").attr("href")
                    download_video(href_video_mp4, one_element.str_id)

                    one_element.shi_pin_url = href_video_mp4

                    browser.close_tabs(video_tab)

                    one_element.to_stander()
                    #现在要将一个数据存到数据库中
                    save_in_db(one_element)
                    one_element_end_time = time.time()
                    adapter.info(
                        f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 爬取结束，用时：{one_element_end_time-one_element_start_time:.2f}")
                    count_all += 1
                    scan_num += 1
                    inpage_count += 1
                    #这里一个结束
                #这里一页结束
                page_next += 1
        #采集结束，关闭当前列表
        browser.close_tabs(list_tab)

    #这里全部列表结束了
    adapter.info(f"本次采集共扫描{scan_num}条记录，更新{update_num}条，新增{count_all}条")
if __name__ == '__main__':
    main()