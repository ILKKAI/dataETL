# -*- coding: utf-8 -*-
"""中国民生银行 理财产品 JRCP_LCCP_ZGMSYH_GW_ALL"""
from database._mongodb import MongoClient


def data_shuffle(data):
    if "PRO_NAME_" not in data:
        return
    # 风险等级
    if "SOURCE_RISK_LEVEL_CODE_" in data:
        if data["SOURCE_RISK_LEVEL_"] == "1":
            data["RISK_LEVEL_CODE_"] = "R1"
        elif data["SOURCE_RISK_LEVEL_"] == "2":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["SOURCE_RISK_LEVEL_"] == "3":
            data["RISK_LEVEL_CODE_"] = "R3"
        elif data["SOURCE_RISK_LEVEL_"] == "4":
            data["RISK_LEVEL_CODE_"] = "R4"
        elif data["SOURCE_RISK_LEVEL_"] == "5":
            data["RISK_LEVEL_CODE_"] = "R5"
    if "START_FUNDS_" in data:
        data["START_FUNDS_"] = data["START_FUNDS_"].replace(",", "")
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGMSYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
