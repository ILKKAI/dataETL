# -*- coding: utf-8 -*-
"""渤海银行理财产品"""
import re

from database._mongodb import MongoClient
from tools.req_for_pdf import parse


def data_shuffle(data):
    result = parse(data["PDF_"])
    pro_name = re.findall(r"产品名称：([\w ]+)", result)
    if pro_name:
        data["PRO_NAME_"] = pro_name[0]
    regist_code = re.findall(r"C\d{13}", result)
    if regist_code:
        data["REGIST_CODE_"] = regist_code[0]
    # 类型
    # pro_type = re.findall(r"产品类型：([\w ]+)", result)
    # if pro_type:
    #     data[""]
    real_days = re.findall(r"期限:(\d+) ?天?", result)
    if real_days:
        data["REAL_DAYS_"] = real_days[0]
    yield_ = re.findall(r"([^:：]+)%", result)
    if yield_:
        if "-" in yield_[0]:
            data["YIELD_HIGH_"] = yield_[0].split("-")[1]
            data["YIELD_LOW_"] = yield_[0].split("-")[0]
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_BHYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
        # quit()
