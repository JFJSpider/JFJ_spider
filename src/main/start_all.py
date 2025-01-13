"""

信息采集的主函数，不停的循环，并读取配置文件中的配置

定时启动正确的脚本

"""
import subprocess
import threading
import time
from queue import Queue
from datetime import datetime, timedelta
import yaml

class ZrclWork:
    def __init__(self,cite_name,full_days,incremental_days,max_duplicate):
        self.cite_name = cite_name
        self.full_days = full_days
        self.incremental_days = incremental_days
        self.max_duplicate = max_duplicate
        self.last_full_work_time = None             #上次全量结束时间
        self.last_incremental_work_time = None      #上次增量结束时间

class ZrclWorkTask:
    def __init__(self,work,is_full_task):
        self.work = work                    #当前任务是什么网站
        self.is_full_task = is_full_task    #本次是否是全量

class ZrclWorkList:
    def __init__(self):
        self.work_list = Queue()     #当前工作列表，列表元素是WORKTASK
        self.all_work = []           #用于保存所有可用的工作，元素是WORK
        self.work_now = None         #当前工作的节点

    def put_work_in_work_list(self,work_task:ZrclWorkTask):
        self.work_list.put(work_task)

    def replace_in_new_work_in_all_work(self,new_work:ZrclWork):
        work_index = 0
        for each_work in self.all_work:
            if each_work.cite_name == new_work.cite_name:
                #如果名字匹配，则替换对应位置为新的工作
                break

        #退出循环后替换
        self.all_work[work_index] = new_work

    def is_work_in_work_list(self,work:ZrclWork):
        for each_work_in_list in self.work_list.queue:
            if work.cite_name == each_work_in_list.work.cite_name:
                #说明当前给定的任务还在列表里
                return True

        #如果遍历完了，就说明不在列表里
        return False

    def update_full_time_date(self,cite_name):
        for each_work in self.all_work:
            if each_work.cite_name == cite_name:
                each_work.last_full_work_time = datetime.now()

    def update_incremental_time_date(self,cite_name):
        for each_work in self.all_work:
            if each_work.cite_name == cite_name:
                each_work.last_incremental_work_time = datetime.now()


def read_config():
    with open('../../script/config/config.ymal', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config

def read_new_config(work_list:ZrclWorkList):
    with open('../script/config/config.ymal', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    for each_cite_config in config['need_get_cites']:
        cite_name = each_cite_config['name']
        full_days = each_cite_config['full_interval_days']
        incremental_days = each_cite_config['incremental_interval_days']
        max_duplicate = each_cite_config['max_retries']
        one_cite = ZrclWork(cite_name,full_days,incremental_days,max_duplicate)
        work_list.replace_in_new_work_in_all_work(one_cite)

def work_list_manager(work_list_need_manage:ZrclWorkList):
    #工作列表管理方法
    #首次启动时将所有网站的增量放入列表，作为第一天的工作
    for work_tmp in work_list_need_manage.all_work:
        tmp_work_task = ZrclWorkTask(work_tmp,is_full_task=False)
        work_list_need_manage.put_work_in_work_list(tmp_work_task)

    #从启动后的第二天开始，每天零点启动，开始规划一日安排
    while True:
        #00:00启动，下面开始规划这一天的任务，放入worklist中
        #将今日需要执行的任务逐个放入worklist中
        now = datetime.now()    #获取当前时间
        next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) #得到下一天的00：00的时间
        delay = (next_run - now).total_seconds()    #将下一轮时间减当前时间，得到总间隔秒数
        time.sleep(delay)   #等待下一天的00:00秒启动

        #启动之后开始安排新的一天的工作
        #首先是读取最新的配置文件
        read_new_config(work_list_need_manage)

        #work_list_need_manage现在是最新的版本的内容
        #根据最新版本分配当日任务
        #先遍历所有任务，判断是否在列表中，如果在列表中，则直接跳过
        #如果不在列表中，则判断是否加入这个新任务
        for each_work in work_list_need_manage.all_work:
            is_now_in_work_list = work_list_need_manage.is_work_in_work_list(each_work)
            if is_now_in_work_list:
                #如果在代办列表中则跳过
                continue
            else:
                #如果不在列表中，则判断是否加入进去
                #首先判断是否到了全量的时间
                need_time_interval = each_work.full_days    #当前配置下的全量时间间隔

                current_date = datetime.now().date()

                date_diff = current_date - each_work.last_full_work_time.date()
                if date_diff >= timedelta(days=need_time_interval):
                    #需要进行全量
                    work_task = ZrclWorkTask(each_work,is_full_task=True)
                    work_list_need_manage.put_work_in_work_list(work_task)  #将全量任务放入工作列表
                    continue    #判断下一个任务

                #如果到这里了，说明现在不需要进行全量，开始判断是否需要增量
                need_time_interval = each_work.incremental_days  # 当前配置下的全量时间间隔

                current_date = datetime.now().date()

                date_diff = current_date - each_work.last_incremental_work_time.date()
                if date_diff >= timedelta(days=need_time_interval):
                    # 需要进行增量
                    work_task = ZrclWorkTask(each_work, is_full_task=False)
                    work_list_need_manage.put_work_in_work_list(work_task)  # 将全量任务放入工作列表
                    continue  # 判断下一个任务

                #到这里说明既不需要全量，也不需要增量直接下一个就行





def main():
    # 主函数是个无限循环
    work_arrangement = ZrclWorkList()

    # 启动时读取配置文件
    config = read_config()

    for each_cite_config in config['need_get_cites']:
        cite_name = each_cite_config['name']
        full_days = each_cite_config['full_interval_days']
        incremental_days = each_cite_config['incremental_interval_days']
        max_duplicate = each_cite_config['max_retries']
        one_cite = ZrclWork(cite_name,full_days,incremental_days,max_duplicate)
        work_arrangement.all_work.append(one_cite)
    #所有配置文件读取完毕

    #启动列表管理子进程
    thread = threading.Thread(target=work_list_manager, args=(work_arrangement,))
    thread.start()

    while True:
        #不断执行代办列表中的事项
        need_process_work_task_now = work_arrangement.work_list.get()
        if need_process_work_task_now.work.cite_name == "dangdang":
            if need_process_work_task_now.is_full_task:
                #如果是增量
                subprocess.run(['python', './../dangdang/dangdang_spider.py', '-m', '1'])
                #执行结束后，更新时间
                work_arrangement.update_full_time_date(need_process_work_task_now.work.cite_name)
            else:
                #如果是全量
                subprocess.run(['python', './../dangdang/dangdang_spider.py', '-m', '0'])
                work_arrangement.update_incremental_time_date(need_process_work_task_now.work.cite_name)

        elif need_process_work_task_now.work.cite_name == "xhsd":
            if need_process_work_task_now.is_full_task:
                #如果是全量
                subprocess.run(['python', './../xhsd/xhsd_postgre.py', '-m', '1'])
                work_arrangement.update_full_time_date(need_process_work_task_now.work.cite_name)
            else:
                #如果是增量
                subprocess.run(['python', './../xhsd/xhsd_postgre.py', '-m', '0','-d', f'{need_process_work_task_now.work.max_duplicate}'])
                work_arrangement.update_incremental_time_date(need_process_work_task_now.work.cite_name)
        elif need_process_work_task_now.work.cite_name == "81sp":
            if need_process_work_task_now.is_full_task:
                #如果是全量
                subprocess.run(['python', './../81sp/81sp_postgre.py', '-m', '1'])
                work_arrangement.update_full_time_date(need_process_work_task_now.work.cite_name)
            else:
                #如果是增量
                subprocess.run(['python', './../81sp/81sp_postgre.py', '-m', '0','-d', f'{need_process_work_task_now.work.max_duplicate}'])
                work_arrangement.update_incremental_time_date(need_process_work_task_now.work.cite_name)
        elif need_process_work_task_now.work.cite_name == "rmwjssp":
            if need_process_work_task_now.is_full_task:
                #如果是全量
                subprocess.run(['python', './../rmwjssp/rmwjssp_postgre.py', '-m', '1'])
                work_arrangement.update_full_time_date(need_process_work_task_now.work.cite_name)
            else:
                #如果是增量
                subprocess.run(['python', './../rmwjssp/rmwjssp_postgre.py', '-m', '0','-d', f'{need_process_work_task_now.work.max_duplicate}'])
                work_arrangement.update_incremental_time_date(need_process_work_task_now.work.cite_name)
        elif need_process_work_task_now.work.cite_name == "rmwxzyjhsp":
            if need_process_work_task_now.is_full_task:
                #如果是全量
                subprocess.run(['python', './../rmwxzyjhsp/rmwxzyjhsp_postgre.py', '-m', '1'])
                work_arrangement.update_full_time_date(need_process_work_task_now.work.cite_name)
            else:
                #如果是全量
                subprocess.run(['python', './../rmwxzyjhsp/rmwxzyjhsp_postgre.py', '-m', '0','-d', f'{need_process_work_task_now.work.max_duplicate}'])
                work_arrangement.update_incremental_time_date(need_process_work_task_now.work.cite_name)
if __name__ == '__main__':
    main()