# -*- coding: utf-8 -*-
"""天天基金网-货币型基金  基金基本信息  CHA_BRANCH_FUND_BASIC"""
from database._mongodb import MongoClient


def data_shuffle(data):
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_TTJJ_GW_HB", mongo_collection="CRMJRCP_JJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
