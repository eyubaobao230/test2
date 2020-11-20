import datetime
import logging
import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler

import pytz


def delay(seconds=None, minutes=None, hours=None, days=None):
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open('end_time.txt', 'a+', encoding='utf-8') as f:
        f.write('采集结束：' + current_time + '\n')

    if seconds:
        logging.warning(
            f'{current_time}，本轮采集已完成,{seconds}秒后进行开始下一轮采集.......................................................')
        time.sleep(seconds)
    elif minutes:
        interval = minutes * 60
        logging.warning(
            f'{current_time}，本轮采集已完成,{minutes}分钟后进行开始下一轮采集.......................................................')
        time.sleep(interval)
    elif hours:
        interval = hours * 60 * 60
        logging.warning(
            f'{current_time}，本轮采集已完成,{hours}小时后进行开始下一轮采集.......................................................')
        time.sleep(interval)
    elif days:
        interval = days * 24 * 60 * 60
        logging.warning(
            f'{current_time}，本轮采集已完成,{days}天后进行开始下一轮采集.......................................................')
        time.sleep(interval)


# 全量采集
def first_crawl(**kwargs):
    os.system("python -m scrapy crawl cwl_history -a is_increment_crawl=0")
    delay(**kwargs)


# 增量采集
def increment_crawl(**kwargs):
    """固定时间间隔启动爬虫(等待前一轮执行完毕才开始计算)"""
    while True:
        os.system("python -m scrapy crawl cwl_history -a is_increment_crawl=1")
        delay(**kwargs)


def start_crawl():
    os.system("python -m scrapy crawl cwl_history -a is_increment_crawl=1")


def scheduler_increment_crawl():
    timezone = pytz.timezone("Asia/Shanghai")
    scheduler = BlockingScheduler()
    nowday = datetime.datetime.now().date()
    scheduler.add_job(start_crawl, 'interval', minutes=1, start_date=f'{nowday} 10:16:00', end_date=f'{nowday} 10:18:30')
    scheduler.start()


if __name__ == '__main__':
    # scheduler_increment_crawl()  # 定时启动任务
    first_crawl(minutes=30)       # 全量数据
    # increment_crawl(minutes=10)     # 增量数据， 在sipder里面修改增量的页数
    #
    # from scrapy import cmdline
    # cmdline.execute("scrapy crawl cwl_history -a is_increment_crawl=1".split())
