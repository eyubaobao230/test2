import datetime
import re

# lottery_draw_time1 = '2020-11-03(二)'
# lottery_draw_time = re.findall('([\d-]+)', lottery_draw_time1)[0]
# print(lottery_draw_time)
# now_time = datetime.datetime.strptime(lottery_draw_time, "%Y-%m-%d") + datetime.timedelta(hours=60*24)
# print(type(now_time))
# print(now_time.strftime("%Y-%m-%d %H:%M:%S"))
# print(type(now_time.strftime("%Y-%m-%d %H:%M:%S")))
typemoney = 1
typemoney = re.sub('（含派奖\d+）|\(含派奖\d+\)', '', typemoney) if '含派奖' in typemoney else typemoney
typemoney = re.sub('（含加奖\d+）|\(含加奖\d+\)', '', typemoney) if '含加奖' in typemoney else typemoney
