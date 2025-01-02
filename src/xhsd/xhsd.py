import argparse
import json
import queue
import random
import re
import threading
import time
from DrissionPage import Chromium,ChromiumOptions
from colorama import Fore, Style,init
from rich.progress import Progress, BarColumn, TextColumn
from rich.color import Color


#id:rmzxb_20240911_67456
class ZrclData:
    def __init__(self):
        self.title = None
        self.author = None
        self.author_universe = None
        self.time = None
        self.meeting_name = None
        self.chair = None
        self.meeting_place = None
        self.download_url = None
        self.theme = None
        self.num = None
        self.url = None
        self.str_id = None


    def to_json(self):
        """
        将 ZrclDataType 类实例转换为 JSON 格式。

        返回:
        - JSON 格式的字符串，包含该实例的所有属性。
        """
        return f"""<REC>
<CD_AUTHOR_EVENT>={self.meeting_name}
<CD_TITLE>={self.title}
<CD_AUTHOR>={self.author}
<CD_UNIVERSE>={self.author_universe}
<CD_DATEYEAR>={self.time}
<CD_MEET_PLACE>={self.meeting_place}
<downloadURL>={self.download_url}
<CD_SERIES_GENERAL>={self.theme}
<CD_INDEX>={self.num}
<CD_ID>={self.str_id}
<CD_COLLECTION_SITE>=美国国家选举研究开放获取资源American National Election Studies
<CD_DOCURL>={self.url}
<REC>
"""

class ZrclSaveData:
    def __init__(self,num_of_list,count,content,num_in_list):
        self.num_of_list = num_of_list  #列表名，这里可以是月份
        self.num_in_list = num_in_list  #月里的日期
        self.count = count  #总计第几个
        self.content = content  #内容类

def gradient_color(percentage):
    """
    通过线性插值生成从蓝色到粉色的渐变颜色。
    percentage: 当前进度百分比 (0~100)
    """
    start_rgb = (0, 128, 255)  # 蓝色
    end_rgb = (255, 0, 128)    # 粉色

    # 线性插值计算 RGB
    r = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * (percentage / 100))
    g = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * (percentage / 100))
    b = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * (percentage / 100))

    return f"#{r:02x}{g:02x}{b:02x}"  # 返回十六进制颜色

class DynamicBarColumn(BarColumn):
    """动态颜色的进度条"""
    def render(self, task):
        percentage = task.percentage or 0
        color = gradient_color(percentage)  # 生成动态颜色
        self.complete_style = f"bold {color}"  # 应用颜色
        return super().render(task)

def rainbow_text(text):
    colors = [Fore.RED, Fore.YELLOW, Fore.GREEN, Fore.CYAN, Fore.BLUE, Fore.MAGENTA]

    for i, char in enumerate(text):
        color = colors[i % len(colors)]
        print(color + char, end='', flush=True)
    print(Style.RESET_ALL)  # 重置样式

def generate_short_unique_id():
    timestamp = int(time.time())  # 当前时间戳（秒级）
    random_part = random.randint(0, 9999)  # 随机数部分
    return f"{timestamp}{random_part}"


def saver(base_save_path,save_queue,all_start_count):
    while True:
        need_save = save_queue.get()
        content = need_save.content.to_json()
        path = base_save_path + f'{need_save.num_of_list}' + '.txt'
        with open(path,'a', encoding='utf-8') as f:
            f.write(content)
            #f.write('\n')
        rainbow_text(f'当前已保存第{all_start_count}个,对应第{need_save.num_of_list}的第{need_save.num_in_list}个')
        all_start_count += 1

def main():

    init()

    #首先启动保存
    count_all = 1
    need_save_queue = queue.Queue()
    base_save_path = 'F:\\Master\\TRS\\trs_web_crawler\\mggjxj\\data\\'
    saver_thread = threading.Thread(target=saver, args=(base_save_path, need_save_queue, count_all), daemon=True)
    saver_thread.start()
    co = ChromiumOptions()
    #co.headless(True)
    co.set_load_mode('eager')
    browser = Chromium(co)
    # browser = Chromium()
    main_tag = browser.latest_tab