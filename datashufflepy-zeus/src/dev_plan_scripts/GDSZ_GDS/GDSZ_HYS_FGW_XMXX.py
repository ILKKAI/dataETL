# -*- coding: utf-8 -*-
"""  GDSZ_HYS_FGW_XMXX"""
from database._mongodb import MongoClient
from tools.req_for_wordExcelZip import find_type


def data_shuffle(data):

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="GDSZ_HYS_FGW_XMXX", mongo_collection="GOV_ZX_GDS")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
