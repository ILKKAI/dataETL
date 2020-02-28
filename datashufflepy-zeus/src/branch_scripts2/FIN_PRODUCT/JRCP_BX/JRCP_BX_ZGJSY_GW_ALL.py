# -*- coding: utf-8 -*-
from database._mongodb import MongoClient


def data_shuffle(data):
    data["ENSURE_SOURCE_TYPE_"] = data["ENSURE_TYPE_"]
    del data["ENSURE_TYPE_"]
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_ZGJSY_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
