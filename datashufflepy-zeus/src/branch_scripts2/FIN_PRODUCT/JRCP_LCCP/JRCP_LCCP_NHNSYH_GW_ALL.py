# -*- coding: utf-8 -*-
"""南海农商银行理财产品 JRCP_LCCP_NHNSYH_GW_ALL"""
import re
from copy import deepcopy

from database._mongodb import MongoClient
from tools.req_for_pdf import parse


def data_shuffle(data):
    re_data = deepcopy(data)

    del re_data['_id']
    risk_level = re_data.get('RISK_LEVEL_')
    # 风险等级
    if risk_level:
        re_data["SOURCE_RISK_LEVEL_"] = risk_level
        if re_data["SOURCE_RISK_LEVEL_"] == "谨慎型" or re_data["SOURCE_RISK_LEVEL_"] == "保守型" or re_data["SOURCE_RISK_LEVEL_"] == "低":
            re_data["RISK_LEVEL_CODE_"] = "R1"
        elif re_data["SOURCE_RISK_LEVEL_"] == "稳健型":
            re_data["RISK_LEVEL_CODE_"] = "R2"
        elif re_data["SOURCE_RISK_LEVEL_"] == "平衡型":
            re_data["RISK_LEVEL_CODE_"] = "R3"
        elif re_data["SOURCE_RISK_LEVEL_"] == "进取型":
            re_data["RISK_LEVEL_CODE_"] = "R4"
        elif re_data["SOURCE_RISK_LEVEL_"] == "激进型":
            re_data["RISK_LEVEL_CODE_"] = "R5"

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_NHNSYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
