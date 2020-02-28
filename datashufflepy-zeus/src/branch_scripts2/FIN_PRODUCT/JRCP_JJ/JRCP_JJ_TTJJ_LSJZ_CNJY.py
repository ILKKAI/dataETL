# -*- coding: utf-8 -*-
"""天天基金网-场内交易基金净值  历史净值  CHA_BRANCH_FUND_DATA"""
from database._mongodb import MongoClient


def data_shuffle(data):
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_TTJJ_LSJZ_CNJY", mongo_collection="CRMJRCP_JJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
