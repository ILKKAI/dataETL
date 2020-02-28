# -*- coding: utf-8 -*-
"""中国银行-官网基金  代销基金  CHA_BRANCH_FUND_AGENT"""

import re
from database._mongodb import MongoClient


def data_shuffle(data):
    # 有些数据PRO_NAME会是空值，舍弃
    if (not data.get("PRO_NAME_")) or "托管协议" in data["PRO_NAME_"]:
        return
    # 因为没有PRO_CODE_所以进行模糊匹配
    elif not data.get("PRO_CODE_"):
        data["PRO_CODE_"] = ""
        s = data["PRO_NAME_"].replace("合同", "")
        data["PRO_NAME_"] = s
        data["PRO_LIKE_NAME_"] = s[:s.find("型")]
    elif data.get("PRO_CODE_") and "A类" in data["PRO_CODE_"]:
        data["PRO_CODE_"] = re.findall(r"\d+", data["PRO_CODE_"])[0]
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_ZGYH_GW_ALL", mongo_collection="CRMJRCP_JJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
