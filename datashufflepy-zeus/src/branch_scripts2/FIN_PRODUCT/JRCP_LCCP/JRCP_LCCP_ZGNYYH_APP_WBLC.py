# -*- coding: utf-8 -*-
from database._mongodb import MongoClient


def data_shuffle(data):

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGNYYH_APP_WBLC", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
