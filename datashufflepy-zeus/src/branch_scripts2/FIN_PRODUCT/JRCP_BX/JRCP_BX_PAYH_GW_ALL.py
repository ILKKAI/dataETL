# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    # re.sub(r"<[^>]+>"," ", s) 替换标签
    if "平安" in data["PRO_NAME_"]:
        data["COM_NAME_"] = "中国平安"
    # if data.get("SPECAIL_"):
    #     data["SPECAIL_"] = re.sub(r"<[^>]+>", " ", data["SPECAIL_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_PAYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
