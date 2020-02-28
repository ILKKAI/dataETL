# -*- coding: utf-8 -*-

# 华夏银行理财产品 HXBFinancial
# 无 CONTENT_  已完成
# 无 产品编码 起始售卖时间 结束售卖时间 风险等级 是否保本 售卖区域 可否赎回 私人银行

import hashlib

import re

import time

import arrow

from scripts import GenericScript


def data_shuffle(data):
    re_data = dict()
    if "TIME_LIMIT_" not in data:
        return

    time_list = data["TIME_LIMIT_"].split("至")
    start_time = time_list[0]
    s_t = arrow.Arrow(year=int(start_time[:4]), month=int(start_time[5:7]), day=int(start_time[8:10]))
    start_time_e = s_t.format("YYYY-MM-DD")

    end_time = time_list[1]
    e_t = arrow.Arrow(year=int(end_time[:4]), month=int(end_time[5:7]), day=int(end_time[8:10]))
    end_time_e = e_t.format("YYYY-MM-DD")

    # "C"
    hash_m = hashlib.md5()
    hash_m.update(data["NAME_"].encode("utf-8"))
    hash_id = hash_m.hexdigest()
    re_data["ID_"] = "HXB" + "_" + hash_id + "_" + str(start_time_e)
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # re_data["AREA_CODE_"]
    re_data["BANK_CODE_"] = "HXB"
    re_data["BANK_NAME_"] = "华夏银行"
    # re_data["UNIT_CODE_"]
    re_data["PERIOD_CODE_"] = str(start_time_e).replace("-", "")
    # re_data["CONTENT_"]
    re_data["STATUS_1"] = ""
    # re_data["REMARK_"]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    # re_data["UPDATE_TIME_"]

    # "F"
    # re_data["CODE_"] = ""
    re_data["NAME_"] = data["NAME_"]
    # 售卖时间范围
    re_data["TIME_LIMIT_"] = data["TIME_LIMIT_"]
    # 收益率
    re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
    # 售卖区域
    # re_data["SALE_DISTRICT_"] = ""
    # 是否保本
    # re_data["BREAKEVEN_"] = ""
    # 起购金额
    re_data["START_FUNDS_"] = re.sub(r"</?\w*>", "", data["START_FUNDS_"])
    # 期限
    re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
    # 开始售卖时间
    re_data["SALE_START_"] = start_time_e
    # 结束售卖时间
    re_data["SALE_END_"] = end_time_e
    # 风险等级
    # re_data["RISK_LEVEL_"] = ""
    re_data["URL_"] = data["URL_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["DATETIME_"] = data["DATETIME_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # 无
    # # 可否赎回
    # re_data["REDEMING_MODE_"]
    # # 私人银行
    # re_data["PRIVATE_BANK_"]
    return re_data


def run():
    script = GenericScript(entity_code="HXBFinancial", entity_type="FINPRODUCT_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        data_shuffle(data)


if __name__ == '__main__':
    run()
