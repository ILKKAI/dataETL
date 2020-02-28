# -*- coding: utf-8 -*-
"""中国光大银行理财产品  JRCP_LCCP_ZGGDYH_GW_ALL"""
from database._mongodb import MongoClient


def data_shuffle(data):
    if data["SOURCE_RISK_LEVEL_"] == "低风险":
        data["RISK_LEVEL_CODE_"] = "R1"
    elif data["SOURCE_RISK_LEVEL_"] == "中低风险":
        data["RISK_LEVEL_CODE_"] = "R2"
    elif data["SOURCE_RISK_LEVEL_"] == "较低风险":
        data["RISK_LEVEL_CODE_"] = "R2"
    elif data["SOURCE_RISK_LEVEL_"] == "中等风险":
        data["RISK_LEVEL_CODE_"] = "R3"
    elif data["SOURCE_RISK_LEVEL_"] == "中高风险":
        data["RISK_LEVEL_CODE_"] = "R4"
    elif data["SOURCE_RISK_LEVEL_"] == "高风险":
        data["RISK_LEVEL_CODE_"] = "R5"
    if not data["PDF_"]:
        del data["PDF_"]
    if "REAL_DAYS_" in data:
        data["REAL_DAYS_"] = data["REAL_DAYS_"].replace("日", "")
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGGDYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
