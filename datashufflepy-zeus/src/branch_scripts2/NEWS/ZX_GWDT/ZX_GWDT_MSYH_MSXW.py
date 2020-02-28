# -*- coding: utf-8 -*-
""""ZX_GWDT_MSYH_MSXW": "中国民生银行|CMBC","""
from database._mongodb import MongoClient


def data_shuffle(data):
    data["BANK_NAME_"] = "中国民生银行"
    data["BANK_CODE_"] = "CMBC"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_MSYH_MSXW", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
