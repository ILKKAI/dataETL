# -*- coding: utf-8 -*-
"""

"""
from database._mongodb import MongoClient


def data_shuffle(data):
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_ZCGG_SZJS_SJSGG", mongo_collection="ZX_ZCGG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
