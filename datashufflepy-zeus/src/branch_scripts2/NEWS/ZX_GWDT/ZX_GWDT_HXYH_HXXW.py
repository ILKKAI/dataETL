# -*- coding: utf-8 -*-
""""ZX_GWDT_HXYH_HXXW": "华夏银行|HB","""
from database._mongodb import MongoClient


def data_shuffle(data):
    data["BANK_NAME_"] = "华夏银行"
    data["BANK_CODE_"] = "HB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_HXYH_HXXW", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
