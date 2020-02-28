# -*- coding: utf-8 -*-
"""
无 PUBLISH_TIME_
"""
from database._mongodb import MongoClient


def data_shuffle(data):
    if "PUBLISH_TIME_" not in data:
        data["PUBLISH_TIME_"] = ""
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_HYBG_BMW_YH", mongo_collection="ZX_HYBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
