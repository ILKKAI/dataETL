# -*- coding: utf-8 -*-
"""交通银行理财产品"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if "YIELD_HIGH_" in data:
        data["YIELD_HIGH_"] = re.sub(r"[\r\n\t]+", "", data["YIELD_HIGH_"])
        if data["YIELD_HIGH_"] == "-":
            del data["YIELD_HIGH_"]
    if "YIELD_LOW_" in data:
        data["YIELD_LOW_"] = re.sub(r"[\r\n\t]+", "", data["YIELD_LOW_"])
        if data["YIELD_LOW_"] == "-":
            del data["YIELD_LOW_"]
    if "SOURCE_RISK_LEVEL_" in data:
        if "1R" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R1"
        elif "2R" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R1"
        elif "3R" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R2"
        elif "4R" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R3"
        elif "5R" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R4"
        elif "6R" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R5"
    # 起购金额
    if "START_FUNDS_" in data:
        # todo 私银客户  其他
        start_funds = re.findall(r"(其他客户[:：].*)", data["START_FUNDS_"])
        if start_funds:
            data["START_FUNDS_"] = start_funds[0]
    # # 递增单位
    # if "INCREASE_" in data:
    #     data["INCREASE_"] = data["INCREASE_"].replace("元", "")

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_JTYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
