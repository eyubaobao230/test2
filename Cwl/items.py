# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


# 历史
class CwlHistoryItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    term = scrapy.Field()
    level = scrapy.Field()
    bonus_type = scrapy.Field()
    match_quantity = scrapy.Field()
    total_stake_number = scrapy.Field()
    alone_stake_price = scrapy.Field()
    total_amount = scrapy.Field()


# 更新
class CwlUpdateItem(scrapy.Item):
    # define the fields for your item here like:
    region = scrapy.Field()
    lottery_type = scrapy.Field()
    lottery_draw_time = scrapy.Field()
    term = scrapy.Field()
    red_ball_number = scrapy.Field()
    blue_ball_number = scrapy.Field()
    total_sales = scrapy.Field()
    total_disbursement_amount = scrapy.Field()
    prize_pool = scrapy.Field()
    deadline_for_drawing_prizes = scrapy.Field()


# 将历史采集到的数据插入最新数据表中
class TotalItem(scrapy.Item):
    level = scrapy.Field()
    lotteryDrawNum = scrapy.Field()
    total_sales = scrapy.Field()
    total_disbursement_amount = scrapy.Field()