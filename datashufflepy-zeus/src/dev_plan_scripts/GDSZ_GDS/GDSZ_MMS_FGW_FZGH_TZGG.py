# -*- coding: utf-8 -*-
"""  GDSZ_MMS_FGW_FZGH_TZGG """
from database._mongodb import MongoClient
from tools.req_for_wordExcelZip import find_type


def data_shuffle(data):
    # data['conten_type'] = find_type(data.get('FJ1_URL_')) if find_type(data.get('FJ1_URL_')) else find_type(data.get('FJ1_NAME_'))

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="GDSZ_MMS_FGW_FZGH_TZGG", mongo_collection="GOV_ZX_GDS")
    data_list = main_mongo.main()
    for data in data_list[:2]:
        re_data = data_shuffle(data)
        print(re_data)
