# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import json
import time
import pymysql

from Cwl.items import CwlUpdateItem, CwlHistoryItem, TotalItem
from Cwl.utils.snowflake import IdWorker


class MySQLPipeline(object):
    def __init__(self, mysql_db, mysql_host, mysql_port, mysql_user, mysql_passwd):

        # MySQL配置
        self.mysql_db = mysql_db
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_passwd = mysql_passwd

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mysql_host=crawler.settings.get("MYSQL_HOST"),
            mysql_user=crawler.settings.get("MYSQL_USER"),
            mysql_passwd=crawler.settings.get("MYSQL_PASSWD"),
            mysql_port=crawler.settings.get("MYSQL_PORT"),
            mysql_db=crawler.settings.get("MYSQL_DB"),
        )

    # 打开数据库
    def open_spider(self, spider):
        self.db_conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, db=self.mysql_db,
                                       user=self.mysql_user, passwd=self.mysql_passwd, charset='utf8')
        self.db_cur = self.db_conn.cursor()

    # 关闭数据库
    def close_spider(self, spider):
        self.db_conn.commit()
        self.db_conn.close()

    # 对数据进行处理
    def process_item(self, item, spider):
        if isinstance(item, CwlHistoryItem):
            name = item["name"]
            term = item["term"]
            if name == 'ssq':
                # 生成id
                worker = IdWorker(1, 0, 0)
                double_chromosphere_id = worker.get_id()
                mysql_table = "double_chromosphere_lottery_draw_record"
                level = item["level"]
                sql = f'select term from {mysql_table} where term=%s and level=%s'
                values = (term, level)
                term_result = self.get_game_id(sql, values)
                if not term_result:     # 不存在数据就插入
                    sql = f"INSERT INTO {mysql_table} (double_chromosphere_id, term, level, total_stake_number, " \
                          f"alone_stake_price, total_amount) VALUES (%s,%s,%s,%s,%s,%s);"
                    values = (double_chromosphere_id, item["term"], item["level"], item["total_stake_number"],
                              item["alone_stake_price"], item["total_amount"])
                    self.insert_db(sql, values)
                else:   # 存在的话就更新, 暂时不知道更新什么, 后期修改
                    pass
            elif name == 'qlc':
                # 生成id
                worker = IdWorker(1, 0, 1)
                seven_lecai_id = worker.get_id()
                mysql_table = "seven_lecai_lottery_draw_record"
                level = item["level"]
                sql = f'select term from {mysql_table} where term=%s and level=%s'
                values = (term, level)
                term_result = self.get_game_id(sql, values)
                if not term_result:     # 不存在数据就插入
                    sql = f"INSERT INTO {mysql_table} (seven_lecai_id, term, level, total_stake_number, " \
                          f"alone_stake_price, total_amount) VALUES (%s,%s,%s,%s,%s,%s);"
                    values = (seven_lecai_id, item["term"], item["level"], item["total_stake_number"],
                              item["alone_stake_price"], item["total_amount"])
                    self.insert_db(sql, values)
                else:   # 存在的话就更新, 暂时不知道更新什么, 后期修改
                    pass
            elif name == '3d':
                worker = IdWorker(1, 0, 2)
                three_dimensional_id = worker.get_id()
                mysql_table = "three_dimensional_lottery_draw_record"
                bonus_type = item["bonus_type"]
                sql = f'select term from {mysql_table} where term=%s and bonus_type=%s'
                values = (term, bonus_type)
                term_result = self.get_game_id(sql, values)
                if not term_result:     # 不存在数据就插入
                    sql = f"INSERT INTO {mysql_table} (three_dimensional_id, term, bonus_type, total_stake_number, " \
                          f"alone_stake_price, total_amount) VALUES (%s,%s,%s,%s,%s,%s);"
                    values = (three_dimensional_id, item["term"], item["bonus_type"], item["total_stake_number"],
                              item["alone_stake_price"], item["total_amount"])
                    self.insert_db(sql, values)
                else:   # 存在的话就更新, 暂时不知道更新什么, 后期修改
                    pass
            elif name == 'kl8':
                worker = IdWorker(1, 0, 3)
                fast_eight_id = worker.get_id()
                mysql_table = "fast_eight_lottery_draw_record"
                bonus_type = item["bonus_type"]
                match_quantity = item["match_quantity"]
                sql = f'select term from {mysql_table} where term=%s and bonus_type=%s and match_quantity=%s'
                values = (term, bonus_type, match_quantity)
                term_result = self.get_game_id(sql, values)
                if not term_result:     # 不存在数据就插入
                    sql = f"INSERT INTO {mysql_table} (fast_eight_id, term, bonus_type, match_quantity, total_stake_number, " \
                          f"alone_stake_price, total_amount) VALUES (%s,%s,%s,%s,%s,%s,%s);"
                    values = (fast_eight_id, item["term"], item["bonus_type"], item["match_quantity"], item["total_stake_number"],
                              item["alone_stake_price"], item["total_amount"])
                    self.insert_db(sql, values)
                else:   # 存在的话就更新, 暂时不知道更新什么, 后期修改
                    pass
        # 更新数据
        elif isinstance(item, CwlUpdateItem):
            worker = IdWorker(1, 0, 1)
            welfare_lottery_id = worker.get_id()
            mysql_table = 'welfare_lottery_lottery_draw'
            term = item["term"]
            lottery_type = item["lottery_type"]
            sql = f'select term from {mysql_table} where term=%s and lottery_type=%s'
            values = (term, lottery_type)
            term_result = self.get_game_id(sql, values)
            if term_result:     # 存在就更新
                update_sql = f"update {mysql_table} set region=%s, lottery_type=%s, lottery_draw_time=%s," \
                             f"term=%s, red_ball_number=%s, blue_ball_number=%s, total_sales=%s, prize_pool=%s, " \
                             f"total_disbursement_amount=%s, deadline_for_drawing_prizes=%s where term=%s and " \
                             f"lottery_type=%s"
                update_values = (item["region"], item["lottery_type"], item["lottery_draw_time"],
                          item["term"], item["red_ball_number"], item["blue_ball_number"], item["total_sales"],
                          item["prize_pool"], item["total_disbursement_amount"], item["deadline_for_drawing_prizes"],
                          term, lottery_type)
                self.update_detail(update_sql, update_values)
            else:   # 不存在就插入
                sql = f"INSERT INTO {mysql_table} (welfare_lottery_id, region, lottery_type, lottery_draw_time," \
                      f" term, red_ball_number, blue_ball_number, total_sales, prize_pool, total_disbursement_amount," \
                      f"deadline_for_drawing_prizes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                values = (welfare_lottery_id, item["region"], item["lottery_type"], item["lottery_draw_time"],
                          item["term"], item["red_ball_number"], item["blue_ball_number"], item["total_sales"],
                          item["prize_pool"], item["total_disbursement_amount"], item["deadline_for_drawing_prizes"])
                self.insert_db(sql, values)

        # 更新插入字段
        elif isinstance(item, TotalItem):
            mysql_table = "welfare_lottery_lottery_draw"
            term = item["lotteryDrawNum"]
            level = item["level"]
            # 查询
            select_sql = f'select term from {mysql_table} where term=%s and lottery_type=%s'
            select_values = (term, level)
            term_result = self.get_game_id(select_sql, select_values)
            if term_result:     # 存在就更新
                update_sql = f"update {mysql_table} set total_sales=%s, total_disbursement_amount=%s where term=%s " \
                             f"and lottery_type=%s"
                update_values = (item["total_sales"], item["total_disbursement_amount"], term, level)
                self.update_detail(update_sql, update_values)
        return item

    # 插入数据
    def insert_db(self, sql, values):
        try:
            self.db_cur.execute(sql, values)
            self.db_conn.commit()
            # print("Insert finished")
        except Exception as e:
            print("插入失败 -- " + str(e) + str(values))
            self.db_conn.commit()
            self.db_conn.close()

    # 查询数据
    def get_game_id(self, sql, values):
        try:
            self.db_cur.execute(sql, values)
            results = self.db_cur.fetchall()
            return results
            # print("Insert finished")
        except Exception as e:
            print("查询失败 -- " + str(e) + str(values))
            self.db_conn.commit()
            self.db_conn.close()
            return 0

    # 更新数据
    def update_detail(self, sql, values):
        try:
            self.db_cur.execute(sql, values)
            self.db_conn.commit()
            # print("Insert finished")
        except Exception as e:
            print("更新失败 -- " + str(e) + str(values))
            self.db_conn.commit()
            self.db_conn.close()
