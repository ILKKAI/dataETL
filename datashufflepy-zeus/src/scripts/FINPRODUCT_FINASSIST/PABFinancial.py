# -*- coding: utf-8 -*-

# 平安银行理财产品 PABFinancial
# 有 CONTENT_ 需清洗  净值公告只有成立日期 和 收益率
import hashlib
import json

import re

import arrow

from scripts import GenericScript


def data_shuffle(data):
    re_data = dict()

    product_name = re.findall(r"(.*?)（产品代码：", data["CONTENT_"])
    product_code = re.findall(r"产品代码：(\w*)", data["CONTENT_"])

    start_time = re.findall(r"于（\w*）成立", data["CONTENT_"])
    if start_time:
        year = re.findall(r"(\d*)年", start_time[0])
        month = re.findall(r"(\d*)月", start_time[0])
        day = re.findall(r"(\d*)日", start_time[0])
        t = arrow.Arrow(year=int(year[0]), month=int(month[0]), day=int(day[0]))
        start_time_e = t.format("YYYY-MM-DD")
    else:
        return None
    yield_rate = re.findall(r"\d\.\d{2}%", data["CONTENT_"])

    # "C"
    hash_m = hashlib.md5()
    hash_m.update(data["NAME_"].encode("utf-8"))
    hash_id = hash_m.hexdigest()
    re_data["ID_"] = "PAB" + "_" + hash_id + "_" + data["DATETIME_"]
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # re_data["AREA_CODE_"]
    re_data["BANK_CODE_"] = "PAB"
    re_data["BANK_NAME_"] = "平安银行"
    # re_data["UNIT_CODE_"]
    re_data["PERIOD_CODE_"] = str(start_time_e).replace("-", "")
    # re_data["CONTENT_"]
    re_data["STATUS_1"] = ""
    # re_data["REMARK_"]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    # re_data["UPDATE_TIME_"]

    # "F"
    re_data["NAME_"] = data["NAME_"]
    # 收益率
    re_data["YIELD_RATE_"] = yield_rate
    # 是否保本
    # re_data["BREAKEVEN_"] = data["BREAKEVEN_"]
    # 起购金额
    # re_data["START_FUNDS_"] = data["START_FUNDS_"]
    # 期限
    re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
    # 结束售卖时间
    re_data["SALE_START_"] = data["SALE_START_"]
    re_data["URL_"] = data["URL_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["DATETIME_"] = data["DATETIME_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # 无
    # re_data["CODE_"] = data["CODE_"]
    # 售卖时间范围
    # re_data["TIME_LIMIT_"] = data["TIME_LIMIT_"]
    # 售卖区域
    # re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
    # 开始售卖时间
    # re_data["SALE_END_"] = data["SALE_END_"]
    # 风险等级
    # re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
    # 可否赎回
    # re_data["REDEMING_MODE_"]
    # 私人银行
    # re_data["PRIVATE_BANK_"]


def run():
    script = GenericScript(entity_code="PABFinancial", entity_type="FINPRODUCT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle(mongo_data_list)


if __name__ == '__main__':
    # run()
    a = '[{"COMMENT_": "新", "REPLIER_TIME_": "2019-01-01 00:28:03", "REPLIER_HEAD_": "https://ww4.sinaimg.cn/orj480/d842991fjw8f893ia3kn2j20no0npq7n.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "永远的冬季2017", "REPLIER_REPLIES_": 0}, {"COMMENT_": "年", "REPLIER_TIME_": "2019-01-01 00:27:59", "REPLIER_HEAD_": "https://ww4.sinaimg.cn/orj480/d842991fjw8f893ia3kn2j20no0npq7n.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "永远的冬季2017", "REPLIER_REPLIES_": 0}, {"COMMENT_": "快", "REPLIER_TIME_": "2019-01-01 00:27:55", "REPLIER_HEAD_": "https://ww4.sinaimg.cn/orj480/d842991fjw8f893ia3kn2j20no0npq7n.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "永远的冬季2017", "REPLIER_REPLIES_": 0}, {"COMMENT_": "乐", "REPLIER_TIME_": "2019-01-01 00:27:49", "REPLIER_HEAD_": "https://ww4.sinaimg.cn/orj480/d842991fjw8f893ia3kn2j20no0npq7n.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "永远的冬季2017", "REPLIER_REPLIES_": 0}, {"COMMENT_": "愁，怎一个愁字了得，过去有首佚名的古诗，首句就是写，生年不满百，常怀千岁忧。这一年国际风云变幻，大致使人都产生"千岁忧",忧患意识充斥国际国内大小会议活动，弥漫在穷人富人各种人心中的上空，科学技术带给人们的富足与便利似乎让人更加担忧未来，不知所措。愁。", "REPLIER_TIME_": "2018-12-31 23:42:26", "REPLIER_HEAD_": "https://ww1.sinaimg.cn/orj480/dd62cdf9jw8f5ivnin92vj20ro0ro40x.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "黑石马", "REPLIER_REPLIES_": 0}, {"COMMENT_": "愁", "REPLIER_TIME_": "2018-12-31 23:27:43", "REPLIER_HEAD_": "https://ww1.sinaimg.cn/orj480/dd62cdf9jw8f5ivnin92vj20ro0ro40x.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "黑石马", "REPLIER_REPLIES_": 0}, {"COMMENT_": "痛，没进农行", "REPLIER_TIME_": "2018-12-31 22:29:45", "REPLIER_HEAD_": "https://wx2.sinaimg.cn/orj480/c5166421ly8fxwsc19pqyj20e80e8mxu.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "lx唐嫣", "REPLIER_REPLIES_": 0}, {"COMMENT_": "喜，累 忙碌并快乐着", "REPLIER_TIME_": "2018-12-31 21:50:26", "REPLIER_HEAD_": "https://wx2.sinaimg.cn/orj480/cf6dbe26ly8ftafgb1gmgj20qo0qotb0.jpg", "REPLIER_PRAISES_": 0, "REPLIER_": "感受生活_随遇而安", "REPLIER_REPLIES_": 0}]'
    b = json.dumps(a)
    print(b)
    print(type(b))