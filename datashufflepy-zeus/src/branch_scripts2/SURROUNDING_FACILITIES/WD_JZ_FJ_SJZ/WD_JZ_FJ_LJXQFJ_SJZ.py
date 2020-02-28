# -*- coding: utf-8 -*-
"""链家-小区-小区房价-石家庄 WD_JZ_FJ_LJXQFJ_SJZ"""
from copy import deepcopy
from database._mongodb import MongoClient


def data_shuffle(data):
    data["TITLE_"] = data["NAME_"]
    re_data = deepcopy(data)
    del re_data['_id']
    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="WD_JZ_FJ_LJXQFJ_SJZ", mongo_collection="WD_JZ_FJ_SJZ")

    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)

