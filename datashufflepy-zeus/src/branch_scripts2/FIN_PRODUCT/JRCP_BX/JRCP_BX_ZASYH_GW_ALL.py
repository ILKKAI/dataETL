# -*- coding: utf-8 -*-
"""招商银行保险"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    # re.sub(r"<[^>]+>","|", s) 替换标签
    # data["ENSURE_CONTENT_"] = ",".join(re.findall(r"[\u4e00-\u9fa5]+", data["ENSURE_CONTENT_"]))
    return data
    # return None


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_ZASYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    # print(data_list[0])
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data["ENSURE_CONTENT_"])
