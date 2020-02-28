# -*- coding: utf-8 -*-
"""中信银行理财产品 JRCP_LCCP_ZXYH_GW_ALL"""
import re

from database._mongodb import MongoClient
from tools.req_for_api import req_for_something


def data_shuffle(data):
    if data["PDF_"][-4:] == "html" or data["PDF_"][:-4] == "HTML":
        response = req_for_something(url=data["PDF_"])
        data["HTML_"] = response.content.decode("gbk")
        data["HTML_NAME_"] = data["PDF_NAME_"]
        regist_code = re.findall(r"C\d{13}", data["HTML_"])
        if regist_code:
            data["REGIST_CODE_"] = regist_code[0]
        else:
            regist_code = re.findall(r"C\d+C\d+", data["HTML_"])
            if regist_code:
                data["REGIST_CODE_"] = "".join(["C", regist_code[0].replace("C", "")])
        del data["PDF_"]
    if "RISK_LEVEL_" in data:
        if data["RISK_LEVEL_"] == "低风险":
            data["RISK_LEVEL_CODE_"] = "R1"
        elif data["RISK_LEVEL_"] == "中低风险":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["RISK_LEVEL_"] == "较低风险":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["RISK_LEVEL_"] == "中等风险":
            data["RISK_LEVEL_CODE_"] = "R3"
        elif data["RISK_LEVEL_"] == "中高风险":
            data["RISK_LEVEL_CODE_"] = "R4"
        elif data["RISK_LEVEL_"] == "高风险":
            data["RISK_LEVEL_CODE_"] = "R5"
    elif "SOURCE_RISK_LEVEL_" in data:
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
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZXYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
