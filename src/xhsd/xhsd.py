import argparse
import time
from datetime import datetime
from decimal import Decimal,ROUND_HALF_UP

from DrissionPage import Chromium,ChromiumOptions
import requests
import base64
import mysql.connector
import logging

#id:rmzxb_20240911_67456
class ZrclData:
    def __init__(self):
        self.str_id = None                  #id
        self.url = None                     #详情页url
        self.title = None                   #书名
        self.price = None                   #价格
        self.author = None                  #作者
        self.zuo_zhe_jian_jie = None        #作者简介
        self.publisher = None               #出版社
        self.image_64 = None                #封面base64
        self.image_url = None               #封面图片
        self.isbn = None                    #isbn
        self.chu_ban_shi_jian = None        #出版时间
        self.fa_xing_fan_wei = None         #发行范围
        self.ye_shu = None                  #页数
        self.zhong = None                   #重量
        self.cip = None                     #CIP核字
        self.gao = None                     #高度
        self.yu_zhong = None                #正文语种
        self.zhong_tu_fen_lei_hao = None    #中图分类号
        self.kai_ben = None                 #开本
        self.yin_shua_shi_jian = None       #印刷时间
        self.bao_zhuang = None              #包装
        self.chu_ci = None                  #出次
        self.zi_shu = None                  #字数
        self.shou_ban_shi_jian = None       #首版时间
        self.yin_zhang = None               #印张
        self.yin_ci = None                  #印次
        self.chu_di = None                  #出地
        self.kuan = None                    #宽
        self.chang = None                   #长
        self.du_zhe_dui_xiang = None        #读者对象
        self.mei_zhi = None                 #媒质
        self.yong_zhi = None                #用纸
        self.shi_fou_zhu_yin = None         #是否注音
        self.bian_zhe = None                #编者
        self.ying_yin_ban_ben = None        #影印版本
        self.chu_ban_shang_guo_bie = None   #出版商国别
        self.ping_lun_shu = None            #评论数
        self.nei_rong_tui_jian = None       #内容推荐
        self.mu_lu = None                   #目录
        self.yi_zhe = None                  #译者
        self.xu_yan = None                  #序言
        self.yin_shu = None                 #印数
        self.fu_zeng_ji_shu_liang = None    #附赠及数量
        self.zheng_li = None                #整理
        self.shi_fou_tao_zhuang = None      #是否套装
        self.hui_zhe = None                 #绘者
        self.jiao_zhu = None                #校注

    def to_stander(self):
        """
        将 ZrclDataType 类实例转换为 数据库 格式。
        """
        #时间格式化
        #印刷时间，出版时间，首版时间
        if self.yin_zhang:
            self.yin_zhang = float(self.yin_zhang)






def get_base64_from_url(url):
    response = requests.get(url)
    base64_data = base64.b64encode(response.content).decode('utf-8')
    return base64_data

def is_duplicate(str_id):
    #这里是判断给定的id，书名，ISBN是否已经在数据库中存在
    connection = mysql.connector.connect(
        host="localhost",  # 数据库主机地址
        user="root",  # 用户名
        password="991201",  # 密码
        database="reslib"  # 数据库名称
    )
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM reslib.rs_correct_resources WHERE str_id='{str_id}'")
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        title_index = columns.index('title')
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

def update_price(str_id, price):
    connection = mysql.connector.connect(
        host="localhost",  # 数据库主机地址
        user="root",  # 用户名
        password="991201",  # 密码
        database="reslib"  # 数据库名称
    )
    connection.autocommit = False   #非自动提交
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM reslib.rs_correct_resources WHERE str_id='{str_id}'")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    price_index = columns.index('price')
    if rows:
        row_now = rows[0]
        price_before = str(row_now[price_index]) #获取过去价格
        if price_before != price:
            #说明价格具有变化
            #需要更新价格

            #首先是价格标准化
            price = Decimal(price)
            price = price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            cursor = connection.cursor()
            cursor.execute(f"""UPDATE reslib.rs_correct_resources SET price='{price}', update_time='{datetime.now()}'  WHERE str_id='{str_id}'""")

            if cursor.rowcount == 1:
                #若只更新了1行，就提交
                connection.commit()
                return True,1
            elif cursor.rowcount > 1:
                connection.rollback()
                return False,2
            else:
                connection.rollback()
                return False,0

        else:
            #说明价格没变化
            return False,-1

def save_in_db(need_save:ZrclData):
    #将当前的数据保存进入DB
    connection = mysql.connector.connect(
        host="localhost",  # 数据库主机地址
        user="root",  # 用户名
        password="991201",  # 密码
        database="reslib"  # 数据库名称
    )
    connection.autocommit = False  # 非自动提交
    cursor = connection.cursor()
    data_dict = {
        "str_id": need_save.str_id,
        "page_url": need_save.url,
        "title": need_save.title,
        "price": need_save.price,
        "author": need_save.author,
        "author_intro": need_save.zuo_zhe_jian_jie,
        "publish": need_save.publisher,
        "base64_url": need_save.image_64,
        "image_url": need_save.image_url,
        "ISBN": need_save.isbn,
        "publish_time": need_save.chu_ban_shi_jian,
        "distribution_scope": need_save.fa_xing_fan_wei,
        "pages": need_save.ye_shu,
        "weight": need_save.zhong,
        "CIP": need_save.cip,
        "height": need_save.gao,
        "language": need_save.yu_zhong,
        "CLCI": need_save.zhong_tu_fen_lei_hao,
        "format": need_save.kai_ben,
        "printing_date": need_save.yin_shua_shi_jian,
        "packages": need_save.bao_zhuang,
        "out_edition": need_save.chu_ci,
        "words": need_save.zi_shu,
        "first_publish_time": need_save.shou_ban_shi_jian,
        "printing_sheet": need_save.yin_zhang,
        "printing_edition": need_save.yin_ci,
        "publish_place": need_save.chu_di,
        "length": need_save.chang,
        "width": need_save.kuan,
        "target_audience": need_save.du_zhe_dui_xiang,
        "book_medium": need_save.mei_zhi,
        "use_paper": need_save.yong_zhi,
        "is_phonetic": need_save.shi_fou_zhu_yin,
        "editor": need_save.bian_zhe,
        "photocopy_version": need_save.ying_yin_ban_ben,
        "publisher_country": need_save.chu_ban_shang_guo_bie,
        "evaluation_number": need_save.ping_lun_shu,
        "translator": need_save.yi_zhe,
        "content_intro": need_save.nei_rong_tui_jian,
        "catalogue": need_save.mu_lu,
        "book_type": 1,
        "data_status":1,
        "data_type":"新华书店",
        "create_time":datetime.now(),
        "update_time":datetime.now(),
        "preface":need_save.xu_yan,
        "printing_num":need_save.yin_shu,
        "freebie_quantity":need_save.fu_zeng_ji_shu_liang,
        "compiler":need_save.zheng_li,
        "is_set":need_save.shi_fou_tao_zhuang,
        "drawer":need_save.hui_zhe,
        "annotation": need_save.jiao_zhu
    }
    str_sql_head = 'INSERT INTO reslib.rs_correct_resources'
    str_sql_middle = ""
    str_sql_value = ""
    value_list = []
    for key,value in data_dict.items():
        if value is not None:
            str_sql_middle +=  key + ","
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
        logging.FileHandler("xhsd.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def main():
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description="新华书店数据采集器")
    parser.add_argument("-m", type=int, default=0 , help="选择采集模式, 0为增量采集, 1为全量采集, 默认为0")
    parser.add_argument("-d", type=int, default=50, help="增量模式下该参数有用，表示最大重复数，重复多少条记录后停止，默认为50")
    args = parser.parse_args()
    mode = args.m    #0表示增量模式，1为全量更新
    max_duplicate_time = args.d
    if mode == 0:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Increment Mode]'})
    else:
        adapter = ModeLoggerAdapter(logger, extra={'mode': '[Full Mode]'})

    if mode == 0:
        adapter.info(f"新华书店信息采集-增量模式-启动")
    if mode == 1:
        adapter.info(f"新华书店信息采集-全量模式-启动")


    count_all = 0
    scan_num = 0
    update_num = 0
    co = ChromiumOptions()
    #co.headless(True)
    #co.set_load_mode('eager')
    browser = Chromium(co)
    # browser = Chromium()
    main_url = ["https://search.xhsd.com/search?frontCategoryId=198","https://search.xhsd.com/search?frontCategoryId=199"]
    list_count = -1
    for each_url in main_url:
        duplicate_time = 0  # 连续的重复记录数
        list_count += 1
        if list_count == 0:
            adapter.info(f"列表1-中国军事开始采集")
        if list_count == 1:
            adapter.info(f"列表2-军事理论开始采集")
        if duplicate_time >= max_duplicate_time:
            # 如果重复次数大于等于50 则退出该列表
            adapter.warning(f"连续重复数大于等于{max_duplicate_time}，列表{list_count+1}增量采集结束")
            continue
        #打开当前列表
        list_tab = browser.new_tab(each_url)

        #点击上架时间按钮
        span_availability_time = list_tab.ele(".filter-banner").ele("上架时间")
        span_availability_time.click(by_js=True)
        time.sleep(1)

        #获取最大页码
        max_page_num = int(list_tab.ele(".pagination").ele("@class=ep").text)
        page_next = 1

        while page_next <= max_page_num:
            adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页开始采集")
            if duplicate_time >= 50:
                #如果重复次数大于等于50 则退出
                break

            #每个循环是一页
            #首先是进入正确的页码判断
            while True:
                real_page_now = int(list_tab.ele(".pagination").ele("@class=current").text)
                if real_page_now == page_next:
                    adapter.info(f"进入了正确的页码：{page_next}")
                    break
                adapter.info(f"调整进入正确页码中...")
                input_page_box = list_tab.ele(".pagination").ele("@tag()=input")
                input_page_box.input(page_next)
                btn_go = list_tab.ele(".pagination").ele(".btn")
                btn_go.click(by_js=True)
                time.sleep(1)

            #这里进入了正确的页码，page_next相当于page_now
            li_all_book = list_tab.ele(".list list-squared list-condensed clearfix").child("@tag()=ul").children("@tag()=li")
            inpage_count = 1
            for li_each_book in li_all_book:
                if duplicate_time >= max_duplicate_time:
                    break

                one_element_start_time = time.time()
                adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个开始采集")

                id_now = li_each_book.attr("data-id")
                id_now = f"xhsd_{id_now}"
                duplicate_flag,book_title = is_duplicate(id_now)
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
                        #如果是全量模式，那就更新一下价格

                        price_now = li_each_book.child(".:product-price")("t=span").text
                        is_update,infect_row = update_price(id_now,price_now)

                        if is_update:
                            #输出日志，是否更新与是否更新成功
                            update_num += 1
                            adapter.info(f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 重复已更新成功！")
                            scan_num += 1
                            inpage_count += 1
                        else:
                            if infect_row == -1:
                                #说明价格没变
                                adapter.info(
                                    f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 重复，无需更新")
                                scan_num += 1
                                inpage_count += 1
                            else:
                                #说明更新出错
                                adapter.error(
                                    f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 重复!更新出错:")
                                scan_num += 1
                                inpage_count += 1
                                if infect_row == 2:
                                    adapter.error(
                                        f"有更新，但更新的目标行是1个以上！已回滚 str_id:{id_now}")
                                if infect_row == 0:
                                    adapter.error(
                                        f"有更新，但更新的目标行是0！已回滚 str_id:{id_now}")

                    else:
                        #暂时不支持的模式
                        raise Exception("暂不支持的模式参数，请检查参数设置")
                    continue

                else:
                    #如果没重复
                    duplicate_time = 0
                one_element = ZrclData()
                #每个循环是一本书
                a_book_now = li_each_book.ele(".product-desc").ele("@tag()=a")

                #打开一本书的页面
                while True:
                    article_tab = browser.new_tab(a_book_now.attr("href"))
                    try:
                        time.sleep(2)
                        one_element.ping_lun_shu = int(article_tab.ele(".comment-tab").ele("t=span").text.strip("(").strip(")"))
                        break
                    except:
                        article_tab.close()

                #url
                one_element.url = article_tab.url

                #id
                article_id = article_tab.ele("@id=id").attr("value")
                one_element.str_id = f"xhsd_{article_id}"

                #title
                one_element.title = article_tab.ele("@tag()=h1").attr("title")

                #出版社 作者
                span_chu_ban_and_zuo_zhe = article_tab.ele("@tag()=h1").nexts("@tag()=label")

                for each_chu_ban in span_chu_ban_and_zuo_zhe:
                    tmp_span_text = each_chu_ban.text
                    if '编' in tmp_span_text:
                        one_element.publisher = each_chu_ban.ele("@tag()=a").text
                        continue
                    if '著' in tmp_span_text:
                        one_element.author = each_chu_ban.ele("@tag()=a").text
                        continue

                    adapter.critical(f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 编著出现了未知字段！str_id={one_element.str_id}")
                    raise Exception(f"编著行出现了未知字段in:{one_element.url}")
                #价格
                one_element.price = article_tab.ele(".item-detail-parent").ele(".item-price").child("t=span").text.replace("￥","")

                #封面部分
                image_img = article_tab.ele(".item-detail-parent").ele(".image-container").ele("@tag()=img")
                one_element.image_64 = get_base64_from_url(image_img.attr("src"))
                one_element.image_url = image_img.attr("src")

                #列表部分
                tr_all_attribute = article_tab.ele(".attribute-list").child(".attribute-tab").child("t=tbody").children("t=tr")
                for tr_each_attribute in tr_all_attribute:
                    text_each_attribute = tr_each_attribute.child("t=td").text

                    if '商品编码（ISBN）' in text_each_attribute:
                        one_element.isbn = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '出版时间' in text_each_attribute:
                        one_element.chu_ban_shi_jian = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '出版社' in text_each_attribute:
                        one_element.publisher = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '印数' in text_each_attribute:
                        one_element.yin_shu = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '媒质' in text_each_attribute:
                        one_element.mei_zhi = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '用纸' in text_each_attribute:
                        one_element.yong_zhi = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '是否注音' in text_each_attribute:
                        one_element.shi_fou_zhu_yin = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '作者' in text_each_attribute:
                        one_element.author = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '绘者' in text_each_attribute:
                        one_element.hui_zhe = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '校注' in text_each_attribute:
                        one_element.jiao_zhu = tr_each_attribute.eles("t=td")[1].text.replace("校注:","").strip()
                        continue

                    if '发行范围' in text_each_attribute:
                        one_element.fa_xing_fan_wei = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '整理' in text_each_attribute:
                        one_element.zheng_li = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '附赠及数量' in text_each_attribute:
                        one_element.fu_zeng_ji_shu_liang = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '是否套装' in text_each_attribute:
                        one_element.shi_fou_tao_zhuang = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '页数' in text_each_attribute:
                        one_element.ye_shu = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '重' in text_each_attribute:
                        one_element.zhong = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if 'CIP核字' in text_each_attribute:
                        one_element.cip = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '高' in text_each_attribute:
                        one_element.gao = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '正文语种' in text_each_attribute:
                        one_element.yu_zhong = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '中图分类号' in text_each_attribute:
                        one_element.zhong_tu_fen_lei_hao = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '中图法分类号' in text_each_attribute:
                        one_element.zhong_tu_fen_lei_hao = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '开本' in text_each_attribute:
                        one_element.kai_ben = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '印刷时间' in text_each_attribute:
                        one_element.yin_shua_shi_jian = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '包装' in text_each_attribute:
                        one_element.bao_zhuang = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '出次' in text_each_attribute:
                        one_element.chu_ci = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '字数' in text_each_attribute:
                        one_element.zi_shu = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '首版时间' in text_each_attribute:
                        one_element.shou_ban_shi_jian = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '印张' in text_each_attribute:
                        one_element.yin_zhang = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '印次' in text_each_attribute:
                        one_element.yin_ci = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '出地' in text_each_attribute:
                        one_element.chu_di = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '宽' in text_each_attribute:
                        one_element.kuan = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '长' in text_each_attribute:
                        one_element.chang = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '影印版本' in text_each_attribute:
                        one_element.ying_yin_ban_ben = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '读者对象' in text_each_attribute:
                        one_element.du_zhe_dui_xiang = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '出版商国别' in text_each_attribute:
                        one_element.chu_ban_shang_guo_bie = tr_each_attribute.eles("t=td")[1].text.strip()
                        continue

                    if '编者' in text_each_attribute:
                        one_element.bian_zhe = tr_each_attribute.eles("t=td")[1].text.replace("编者:","").strip()
                        continue

                    if '译者' in text_each_attribute:
                        one_element.yi_zhe = tr_each_attribute.eles("t=td")[1].text.replace("译者:", "").strip()
                        continue

                    adapter.critical(
                        f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 表格出现了未知字段！str_id={one_element.str_id}")
                    raise Exception(f"列表中出现了新的字段：{text_each_attribute} in url:{one_element.url}")

                #内容简介
                div_details = None
                try:
                    div_details = article_tab.ele(".detail-floors",timeout=1).children("t=div",timeout=1)
                except Exception as e:
                    adapter.debug(
                        f"列表{list_count + 1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 没有详情！str_id={one_element.str_id}")

                if div_details is not None:
                    for div_each_detail in div_details:
                        h2_each_detail = div_each_detail.ele(".floor-title").ele("t=h2")

                        if "目录" in h2_each_detail.text:
                            one_element.mu_lu = h2_each_detail.parent().next().text.strip()
                            continue
                        if "内容推荐" in h2_each_detail.text:
                            one_element.nei_rong_tui_jian = h2_each_detail.parent().next().text.strip()
                            continue

                        if "作者简介" in h2_each_detail.text:
                            one_element.zuo_zhe_jian_jie = h2_each_detail.parent().next().text.strip()
                            continue

                        if "序言" in h2_each_detail.text:
                            if one_element.xu_yan is None:
                                one_element.xu_yan = ""
                            one_element.xu_yan += h2_each_detail.parent().next().text.strip() + ' | '
                            continue

                        if "导语" in h2_each_detail.text:
                            if one_element.xu_yan is None:
                                one_element.xu_yan = ""
                            one_element.xu_yan += h2_each_detail.parent().next().text.strip() + ' | '
                            continue

                        if "精彩页" or "后记" in h2_each_detail.text:
                            continue

                        adapter.critical(
                            f"列表{list_count+1}-{page_next}/{max_page_num}页-页内第{inpage_count}个 书本详情出现了未知字段！str_id={one_element.str_id}")
                        raise Exception(f"书本详情出现了新的字段：{h2_each_detail.text} in url:{one_element.url}")

                if one_element.xu_yan is not None:
                    one_element.xu_yan = one_element.xu_yan.rstrip(" | ")
                #评论数
                one_element.ping_lun_shu = int(article_tab.ele(".comment-tab").ele("t=span").text.strip("(").strip(")"))
                #书爬取结束，关闭页面
                browser.close_tabs(article_tab)

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