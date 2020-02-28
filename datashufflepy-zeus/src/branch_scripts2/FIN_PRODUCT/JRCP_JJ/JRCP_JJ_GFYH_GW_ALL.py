# -*- coding: utf-8 -*-
"""广发银行-官网基金信息  代销基金  CHA_BRANCH_FUND_AGENT"""
from database._mongodb import MongoClient


def data_shuffle(data):
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_GFYH_GW_ALL", mongo_collection="CRMJRCP_JJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
