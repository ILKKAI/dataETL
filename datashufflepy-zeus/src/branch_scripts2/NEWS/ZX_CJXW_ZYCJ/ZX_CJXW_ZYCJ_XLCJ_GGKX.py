# -*- coding: utf-8 -*-
"""新浪财经-港股-市场快讯及分析 ZX_CJXW_ZYCJ_XLCJ_GGKX"""
import re
from database._mongodb import MongoClient


def data_shuffle(data):
    data["CONTENT_"] = re.sub(r"\.ct_hqimg[^\u4e00-\u9fa5]+", "", data["CONTENT_"])

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_CJXW_ZYCJ_XLCJ_GGKX", mongo_collection="ZX_CJXW_ZYCJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
