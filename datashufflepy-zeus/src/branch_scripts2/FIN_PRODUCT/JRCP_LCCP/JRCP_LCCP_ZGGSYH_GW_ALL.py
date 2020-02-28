# -*- coding: utf-8 -*-
"""中国工商银行 理财产品 JRCP_LCCP_ZGGSYH_GW_ALL"""
import re

from database._mongodb import MongoClient
from tools.req_for_api import req_for_something


def data_shuffle(data):
    if "RAISE_START_" in data:
        data["RAISE_START_"] = re.sub(r"[^\d-]", "", data["RAISE_START_"])
    if "YIELD_HIGH_" in data:
        if "-" in data["YIELD_HIGH_"]:
            yield_rate = data["YIELD_HIGH_"].split("-")
            data["YIELD_HIGH_"] = yield_rate[0]
            data["YIELD_LOW_"] = yield_rate[1]
    # 起购金额
    if "START_FUNDS_" in data:
        data["START_FUNDS_"] = data["START_FUNDS_"].replace("W", "0000")
    # 风险等级
    if "SOURCE_RISK_LEVEL_" in data:
        if "很低" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R1"
        elif "较低" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R2"
        elif "中低" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R3"
        elif "中高" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R4"
        elif "较高" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R4"
        elif "很高" in data["SOURCE_RISK_LEVEL_"]:
            data["RISK_LEVEL_CODE_"] = "R5"
    # PDF
    if "PDF_" in data:
        if data["PDF_"]:
            response = req_for_something(url=data["PDF_"])
            pdf_url = re.findall(r"pdf_filename = \"(.*)\";", response.content.decode("gbk"))
            if pdf_url:
                data["PDF_"] = pdf_url[0]
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGGSYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
