# -*- coding: utf-8 -*-
"""
无 CONTENT_
"""
from database._mongodb import MongoClient


def data_shuffle(data):
    if "CONTENT_" not in data:
        data["CONTENT_"] = ""
    if "HTML_" not in data:
        data["HTML_"] = ""
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_ZCGG_SJS_ZJHL", mongo_collection="ZX_ZCGG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
