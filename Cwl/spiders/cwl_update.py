# -*- coding: utf-8 -*-
import datetime
import json
import logging
import re

import scrapy

from Cwl.items import CwlUpdateItem


class CwlUpdateSpider(scrapy.Spider):
    name = 'cwl_update'
    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://www.cwl.gov.cn/',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'Sites=_21; UniqueID=xamzIfAlpCCjGdbi1604548913419; _ga=GA1.3.970657903.1604548912; _gid=GA1.3.380048980.1604548912; _gat_gtag_UA_113065506_1=1; 21_vq=11'
    }

    def start_requests(self):
        start_urls = [
            'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=ssq&issueCount=1',
            'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=3d&issueCount=1',
            'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=qlc&issueCount=1',
            'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=kl8&issueCount=1',
        ]
        for url in start_urls:
            yield scrapy.Request(url, headers=self.headers, callback=self.parse_info)

    def parse_info(self, response):
        json_res = json.loads(response.text)
        results = json_res["result"]
        for result in results:
            content = result["content"]  # 带解析
            content = re.sub(',共\d+注。|,共\d+注。(.*?)。', '', content)
            area_code = '0'  # 暂定开奖地区为空
            if content:
                content_list = content.split(',')
                if len(content_list) > 0:
                    area_code_list = []
                    for i in content_list:
                        area_name = re.findall('(.*?)\d+注', i)[0]
                        if area_name != '共':
                            # area_code = self.area_code(area_name)
                            # area_code_list.append(str(area_code))
                            area_code_list.append(str(area_name))
                    area_code = ','.join(area_code_list)

            lottery_type1 = result["name"]   # 彩票类型
            lottery_type = 1
            if "双色球" in lottery_type1:
                lottery_type = 1
            elif "七乐彩" in lottery_type1:
                lottery_type = 2
            elif "3D" in lottery_type1:
                lottery_type = 3
            elif "快乐8" in lottery_type1:
                lottery_type = 4
            lottery_draw_time1 = result["date"]  # 2020-11-03(二)
            lottery_draw_time = re.findall('([\d-]+)', lottery_draw_time1)[0] + ' 20:30:00'   # 开奖时间
            term = result["code"]
            red_ball_number = result["red"]     # 红球
            blue_ball_number = result["blue"]   # 蓝球
            blue_ball_number2 = result["blue2"]
            total_sales1 = result["sales"]  # 总销售金额
            try:
                total_sales = round(float(total_sales1))
            except Exception as e:
                logging.warning(f"{str(e)} - total_sales - {total_sales1}")
                total_sales = 0
            total_disbursement_amount = 0      # 总派奖金额
            prize_pool1 = result["poolmoney"]   # 奖池金额
            try:
                prize_pool = round(float(prize_pool1))
            except Exception as e:
                logging.warning(f"{str(e)} - prize_pool - {prize_pool1}")
                prize_pool = 0
            deadline_for_drawing_prizes1 = datetime.datetime.strptime(lottery_draw_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=60 * 24)
            deadline_for_drawing_prizes = deadline_for_drawing_prizes1.strftime("%Y-%m-%d %H:%M:%S")
            area_code = 0
            info_dict = {
                "region": area_code,
                "lottery_type": lottery_type,
                "lottery_draw_time": lottery_draw_time,
                "term": term,
                "red_ball_number": red_ball_number,
                "blue_ball_number": blue_ball_number,
                "total_sales": total_sales,
                "total_disbursement_amount": total_disbursement_amount,
                "prize_pool": prize_pool,
                "deadline_for_drawing_prizes": deadline_for_drawing_prizes,
            }
            info_item = CwlUpdateItem(**info_dict)
            yield info_item

    def area_code(self, name):
        province_dict = {
            '北京': 110000, '天津': 120000, '河北': 130000, '山西': 140000, '内蒙古': 150000, '辽宁': 210000,
            '吉林': 220000, '黑龙江': 230000, '上海': 310000, '江苏': 320000, '浙江': 330000, '安徽': 340000,
            '福建': 350000, '江西': 360000, '山东': 370000, '河南': 410000, '湖北': 420000, '湖南': 430000,
            '广东': 440000, '广西': 450000, '海南': 460000, '重庆': 500000, '四川': 510000, '贵州': 520000,
            '云南': 530000, '西藏': 540000, '陕西': 610000, '甘肃': 620000, '青海': 630000, '宁夏': 640000,
            '新疆': 650000,
        }
        province_code = province_dict[name]
        return province_code
