# -*- coding: utf-8 -*-
""""ZX_GWDT_HFYH_YFXW": "恒丰银行|EBCL","""
from database._mongodb import MongoClient


def data_shuffle(data):
    data["BANK_NAME_"] = "恒丰银行"
    data["BANK_CODE_"] = "EBCL"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_HFYH_YFXW", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
