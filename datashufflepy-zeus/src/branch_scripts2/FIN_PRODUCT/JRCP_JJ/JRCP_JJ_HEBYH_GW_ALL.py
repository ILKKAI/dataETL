# -*- coding: utf-8 -*-
"""JRCP_JJ_HEBYH_GW_ALL 	河北银行-官网保险
"""
import pymongo
from copy import deepcopy

from database._mongodb import MongoClient
import re


def data_shuffle(data, ):
    '''
    处理_id,
    :param data:
    :return:
    '''
    re_data = deepcopy(data)
    del re_data['_id']

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_HEBYH_GW_ALL", mongo_collection="JRCP_JJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
