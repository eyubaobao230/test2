# -*- coding: utf-8 -*-
import datetime
import json
import logging
import re
import scrapy
from Cwl.items import CwlHistoryItem, CwlUpdateItem, TotalItem


class CwlHistorySpider(scrapy.Spider):
    name = 'cwl_history'

    increment_page_limit = 1

    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/86.0.4240.183 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://www.cwl.gov.cn/',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'Sites=_21; UniqueID=xamzIfAlpCCjGdbi1604548913419; _ga=GA1.3.970657903.1604548912; '
                  '_gid=GA1.3.380048980.1604548912; _gat_gtag_UA_113065506_1=1; 21_vq=11'
    }

    def __init__(self, name=name, **kwargs):
        super().__init__(name=name, **kwargs)
        self.increment_crawl = int(kwargs.get('is_increment_crawl'))

    def start_requests(self):
        day_start = '2013-01-01'
        # today = '2015-01-01'
        today = datetime.date.today()
        start_urls = [
            # f'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=ssq&issueCount=&issueStart=&issueEnd=&dayStart={day_start}&dayEnd={today}&pageNo=1',
            # f'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=kl8&issueCount=&issueStart=&issueEnd=&dayStart={day_start}&dayEnd={today}&pageNo=1',
            f'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=3d&issueCount=&issueStart=&issueEnd=&dayStart={day_start}&dayEnd={today}&pageNo=1',
            # f'http://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice?name=qlc&issueCount=&issueStart=&issueEnd=&dayStart={day_start}&dayEnd={today}&pageNo=1',
        ]
        for url in start_urls:
            current_page = 1
            yield scrapy.Request(url, headers=self.headers, callback=self.parse_list,
                                 meta={"url": url, "current_page": current_page})

    def parse_list(self, response):
        json_res = json.loads(response.text)
        url = response.meta["url"]
        current_page = response.meta["current_page"]
        name = re.findall('name=([0-9a-z]+)&', url)[0]
        pageCount = json_res["pageCount"]
        result_list = json_res["result"]
        result_list = result_list[:1] if self.increment_crawl else result_list  # 增量每次取一条
        for result in result_list:
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

            lottery_type1 = result["name"]  # 彩票类型
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

            term = result["code"]
            prizegrades = result["prizegrades"]
            for grade in prizegrades:
                level = grade["type"]
                if lottery_type == 4:
                    bonus_type, match_quantity = grade["type"].replace('x', '').split('z')    # 3d
                else:
                    bonus_type = grade["type"]
                    match_quantity = 0
                typenum = grade["typenum"]
                typemoney = grade["typemoney"]
                typenum = 0 if '_' in typenum else typenum
                typemoney = 0 if '_' in typemoney else typemoney
                try:
                    typemoney = re.sub('（含派奖\d+）|\(含派奖\d+\)', '', typemoney) if '含派奖' in typemoney else typemoney
                    typemoney = re.sub('（含加奖\d+）|\(含加奖\d+\)', '', typemoney) if '含加奖' in typemoney else typemoney
                except Exception as e:
                    print('----  ' + str(typemoney) + str(e))

                if typenum or typemoney:
                    total_stake_number = round(float(typenum)) if typemoney else 0     # zong
                    alone_stake_price = round(float(typemoney)) if typemoney else 0   # dan
                    total_amount = total_stake_number * alone_stake_price   #
                    # if total_amount ==
                    total_disbursement_amount += total_amount   # 所有奖项的奖金综合
                    result_dict = {
                        "name": name,
                        "term": term,
                        "level": level,
                        "bonus_type": bonus_type,
                        "match_quantity": match_quantity,
                        "total_stake_number": total_stake_number,
                        "alone_stake_price": alone_stake_price,
                        "total_amount": total_amount,
                    }

                    result_item = CwlHistoryItem(**result_dict)
                    yield result_item

            # 插入单个
            total_dict = {
                "level": lottery_type,
                "lotteryDrawNum": term,
                "total_sales": total_sales,
                "total_disbursement_amount": total_disbursement_amount,
            }
            total_item = TotalItem(**total_dict)
            yield total_item

            info_dict = {
                "region": 0,
                # "region": area_code,
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

        # 全量
        if not self.increment_crawl and current_page < pageCount:
            current_page += 1
            next_page = re.sub('pageNo=(\d+)', f'pageNo={current_page}', url)
            yield scrapy.Request(next_page, headers=self.headers, callback=self.parse_list,
                                 meta={"current_page": current_page, "url": next_page})
        # 增量
        if self.increment_crawl and current_page < self.increment_page_limit:
            current_page += 1
            next_page = re.sub('pageNo=(\d+)', f'pageNo={current_page}', url)
            yield scrapy.Request(next_page, headers=self.headers, callback=self.parse_list,
                                 meta={"current_page": current_page, "url": next_page})
