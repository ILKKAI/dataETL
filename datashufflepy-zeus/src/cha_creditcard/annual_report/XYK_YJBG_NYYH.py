# -*- coding: utf-8 -*-
"""XYK_YJBG_NYYH  同业年报 中国农业银行"""
from database._mongodb import MongoClient


def data_shuffle(data):
    data["FILE_URL_"] = data["PDF_URL_"]
    data["FILE_NAME_"] = data["PDF_NAME_"]
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="XYK_YJBG_NYYH", mongo_collection="XYK_YJBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
