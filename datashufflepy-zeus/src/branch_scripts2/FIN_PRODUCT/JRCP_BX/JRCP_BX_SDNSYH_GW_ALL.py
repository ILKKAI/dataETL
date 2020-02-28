# -*- coding: utf-8 -*-
"""JRCP_BX_SDNSYH_GW_ALL 	顺德农商银行-官网保险
"""
from copy import deepcopy

from database._mongodb import MongoClient
import pandas as pd
import pymongo
import pymysql
from pandas import Series, DataFrame
import numpy as np
import io

from sqlalchemy import create_engine
import re


def data_shuffle(data, ):
    '''
    处理_id, 爬取时间
    :param data:
    :return:
    '''
    re_data = deepcopy(data)
    del re_data['_id']
    del re_data['ALL_DATA_']
    list_data = []
    excel_data = pd.read_excel(r'C:\Users\xiaozhi\Desktop\jupyter_data_pandas\workspace\files_\查呗数据配置表(9-17)(周郅翔).xlsx', header=0, sheet_name='name')
    for index, row in excel_data.iterrows():
        bx_data = deepcopy(re_data)
        bx_data["COM_NAME_"] = row["COM_NAME_"]
        bx_data["PRO_NAME_"] = row["PRO_NAME_"]
        bx_data["ENSURE_PAY_"] = row["ENSURE_PAY_"]
        bx_data['URL_'] = bx_data['URL_'] + str(row["PRO_NAME_"])
        list_data.append(bx_data)

    return list_data  # 返回一个列表


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_SDNSYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)