# -*- coding: utf-8 -*-
"""  GDSZ_GDS_FGW_FZGH"""
from database._mongodb import MongoClient
from tools.req_for_wordExcelZip import find_type


def data_shuffle(data):
    if data.get('ENTITY_NAME_') == '广东市发改委-发展规划':
        data['ENTITY_NAME_'] = '广东省发改委-发展规划'
    data['conten_type'] = find_type(data.get('FJ1_URL_')) if find_type(data.get('FJ1_URL_')) else find_type(data.get('FJ1_NAME_'))

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="GDSZ_GDS_FGW_FZGH", mongo_collection="GOV_ZX_GDS")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
