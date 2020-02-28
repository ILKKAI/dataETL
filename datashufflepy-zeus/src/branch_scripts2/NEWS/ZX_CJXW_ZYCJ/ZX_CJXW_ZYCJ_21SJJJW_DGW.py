# -*- coding: utf-8 -*-
"""21世纪经济网-大湾港  ZX_CJXW_ZYCJ_21SJJJW_DGW"""
import re
from database._mongodb import MongoClient


def data_shuffle(data):
    data["HTML_"] = re.sub(r"<p class=\"copyright\".*?</p>", "", data["HTML_"])
    data["HTML_"] = re.sub(r"<a.*?返回21经济首页.*?</a>", "", data["HTML_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_CJXW_ZYCJ_21SJJJW_DGW", mongo_collection="ZX_CJXW_ZYCJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
