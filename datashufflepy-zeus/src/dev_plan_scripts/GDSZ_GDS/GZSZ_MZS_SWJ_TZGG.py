# -*- coding: utf-8 -*-
"""  GZSZ_MZS_SWJ_TZGG"""
from database._mongodb import MongoClient
from tools.req_for_wordExcelZip import find_type


def data_shuffle(data):

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="GZSZ_MZS_SWJ_TZGG", mongo_collection="GOV_ZX_GDS")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
