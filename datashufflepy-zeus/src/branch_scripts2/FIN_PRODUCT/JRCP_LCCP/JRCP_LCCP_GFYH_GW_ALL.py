# -*- coding: utf-8 -*-
"""广发银行理财产品 JRCP_GFYH_GW_ALL"""
from database._mongodb import MongoClient


def data_shuffle(data):
    if "R1" in data["SOURCE_RISK_LEVEL_"]:
        data["RISK_LEVEL_CODE_"] = "R1"
    elif "R2" in data["SOURCE_RISK_LEVEL_"]:
        data["RISK_LEVEL_CODE_"] = "R2"
    elif "R3" in data["SOURCE_RISK_LEVEL_"]:
        data["RISK_LEVEL_CODE_"] = "R3"
    elif "R4" in data["SOURCE_RISK_LEVEL_"]:
        data["RISK_LEVEL_CODE_"] = "R4"
    elif "R5" in data["SOURCE_RISK_LEVEL_"]:
        data["RISK_LEVEL_CODE_"] = "R5"
    if "长期" in data["RAISE_START_"]:
        del data["RAISE_START_"]
        del data["RAISE_END_"]
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_GFYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
