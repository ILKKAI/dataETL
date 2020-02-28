# -*- coding: utf-8 -*-
"""华夏银行理财产品  JRCP_LCCP_HXYH_GW_ALL"""
import re

from database._mongodb import MongoClient
from tools.req_for_pdf import parse


def data_shuffle(data):
    result = parse(pdf_url=data["PDF_"])
    # 登记编码
    regist_code = re.findall(r"C\d{13}", result)
    if regist_code:
        data["REGIST_CODE_"] = regist_code[0]
    # 风险等级
    risk_level = re.findall(r"本产品为 ?([\w ]+（\w+）)理财产品", result)
    if risk_level:
        data["SOURCE_RISK_LEVEL_"] = risk_level[0]
        if "R1" in risk_level[0]:
            data["RISK_LEVEL_CODE_"] = "R1"
        elif "R2" in risk_level[0]:
            data["RISK_LEVEL_CODE_"] = "R2"
        elif "R3" in risk_level[0]:
            data["RISK_LEVEL_CODE_"] = "R3"
        elif "R4" in risk_level[0]:
            data["RISK_LEVEL_CODE_"] = "R4"
        elif "R5" in risk_level[0]:
            data["RISK_LEVEL_CODE_"] = "R5"
    else:
        pass
        # print(result)
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_HXYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
