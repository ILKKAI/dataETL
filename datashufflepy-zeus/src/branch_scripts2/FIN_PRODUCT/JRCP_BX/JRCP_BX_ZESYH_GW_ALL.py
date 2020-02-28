# -*- coding: utf-8 -*-
# 浙商银行保险
from database._mongodb import MongoClient


def data_shuffle(data):

    # 浙商银行无保险数据
    return
    # return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_ZESYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
