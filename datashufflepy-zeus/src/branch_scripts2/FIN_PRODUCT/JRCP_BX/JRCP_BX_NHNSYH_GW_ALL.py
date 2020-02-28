# -*- coding: utf-8 -*-
"""JRCP_BX_NHNSYH_GW_ALL 	南海农商银行-官网保险
"""
from copy import deepcopy

from database._mongodb import MongoClient
import re


def data_shuffle(data, ):
    re_data = deepcopy(data)
    re_data['COM_NAME_'] = re.sub(r'\（客服电话.+\）', "", re_data['COM_NAME_'])
    del re_data['_id']
    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_NHNSYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)