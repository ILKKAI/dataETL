# -*- coding: utf-8 -*-
"""CRMJPFX_ZQ_HXZQ 卡宝宝银行-网点"""
from copy import deepcopy

from crm_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, ):
    '''
    处理_id, 爬取时间
    :param data:
    :return:
    '''
    re_data = deepcopy(data)
    # re_data['ID_'] = re_data.get('_id')
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    del re_data['_id']
    del re_data['DEALTIME_']
    del re_data['DATETIME_']
    return re_data


if __name__ == '__main__':

    main_mongo = MongoClient(entity_code="CRMJPFX_XT_YYXTW", mongo_collection="CRMJPFX_XT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data=data,)
        print(re_data)
