# -*- coding: utf-8 -*-
"""JRCP_BX_NHNSYH_GW_ALL 	南海农商银行-官网保险
"""
import pymongo
from copy import deepcopy

from database._mongodb import MongoClient
import re

client = pymongo.MongoClient(host="172.22.69.35", port=20000, serverSelectionTimeoutMS=60, connectTimeoutMS=60, connect=False)
db = client["spider_url_fixed"]

ft_dict = {"股票型": "GPX", "混合型": "HHX", "债券型": "ZQX", "指数型": "ZSX", "ETF链接": "ETFLJ",
               "QDII": "QDII", "LOF": "LOF", "FOF": "FOF", "货币型": "HBX", "理财型": "LCX",
               "分级杠杆": "FJGG", "ETF - 场内": "ETF_CN", "其他": "QT", }


def data_shuffle(data, ):
    '''
    处理_id, 爬取时间
    :param data:
    :return:
    '''
    re_data = deepcopy(data)
    # re_data['ID_'] = re_data.get('_id')
    # re_data["SPIDER_TIME_"] = data["DATETIME_"]
    del re_data['_id']
    re_data['FUND_TYPE_'] = list(db.JRCP_JJ_TTJJ_JZ_ALL.find({'PARAM_.0.value': re_data.get('PRO_CODE_')}))[0].get('PARAM_')[-1].get('value')
    if not re_data['FUND_TYPE_']:
        re_data['FUND_TYPE_'] = list(db.JRCP_JJ_TTJJ_FJZ_ALL.find({'PARAM_.0.value': re_data.get('PRO_CODE_')}))[0].get('PARAM_')[-1].get('value')

    re_data['FUND_TYPE_CODE_'] = ft_dict.get(re_data['FUND_TYPE_'])

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_NHYH_GW_ALL", mongo_collection="CRMJRCP_JJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
