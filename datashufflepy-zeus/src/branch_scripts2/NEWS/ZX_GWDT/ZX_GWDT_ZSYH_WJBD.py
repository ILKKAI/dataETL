# -*- coding: utf-8 -*-
""""ZX_GWDT_ZSYH_WJBD": "浙商银行|CZB","""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data["CONTENT_"]:
        data["CONTENT_"] = re.sub(r"\.TRS_Editor[A-Za-z]+{.*?}", "", data["CONTENT_"])
    data["BANK_NAME_"] = "浙商银行"
    data["BANK_CODE_"] = "CZB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_ZSYH_WJBD", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
