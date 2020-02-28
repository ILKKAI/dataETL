# -*- coding: utf-8 -*-
"""中国银行-中行动态 ZX_GWDT_ZGYH_ZHDT"""
""""ZX_GWDT_ZGYH_ZHDT": "中国银行|BOC","""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data["CONTENT_"]:
        data["CONTENT_"] = re.sub(r"\|varapp.*?\|", "", data["CONTENT_"])
    data["BANK_NAME_"] = "中国银行"
    data["BANK_CODE_"] = "BOC"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_ZGYH_ZHDT", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
