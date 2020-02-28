# -*- coding: utf-8 -*-
"""中国企业家网-人物 ZX_CJXW_GJJRJG_ZGYYJW_RW"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    data["HTML_"] = re.sub(r"<p style = \"color: #888888; font-size: 12px;\">.*</p>", "", data["HTML_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_CJXW_GJJRJG_ZGYYJW_RW", mongo_collection="ZX_CJXW_GJJRJG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
