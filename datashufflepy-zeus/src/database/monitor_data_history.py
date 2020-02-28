# -*- coding: utf-8 -*-
import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-9])
import datetime
import json
import pymongo
import time
from database._phoenix_hbase import *
import openpyxl

# client = pymongo.MongoClient(["192.168.1.73:27017","192.168.1.81:27017","192.168.1.102:27017"]
#                              ,serverSelectionTimeoutMS=60,connectTimeoutMS=60, connect=False)
phoenix = PhoenixHbase('')


def get_day(date, step=0):
    """获取指定日期date(形如"xxxx-xx-xx")之前或之后的多少天的日期, 返回值为字符串格式的日期"""
    l = date.split("-")
    y = int(l[0])
    m = int(l[1])
    d = int(l[2])
    old_date = datetime.datetime(y, m, d)
    new_date = (old_date + datetime.timedelta(days=step)).strftime('%Y-%m-%d')
    return new_date


if __name__ == '__main__':
    connection = phoenix.connect_to_phoenix()
    phoenix_curs = connection.cursor()
    table_list = ['CHA_BRANCH_BUSINESS',
                  'CHA_BRANCH_BUS_STATION',
                  'CHA_BRANCH_CREDITCARDARD',
                  'CHA_BRANCH_FACILITY',
                  'CHA_BRANCH_FINANCIAL_PRODUCT',
                  'CHA_BRANCH_FUND_AGENCY',
                  'CHA_BRANCH_FUND_BASIC',
                  'CHA_BRANCH_HOSPITAL',
                  'CHA_BRANCH_HOUSE',
                  'CHA_BRANCH_HOUSE_DATA',
                  'CHA_BRANCH_HOUSE_PRICE',
                  'CHA_BRANCH_INSURANCE',
                  'CHA_BRANCH_MAIN_ROUTE',
                  'CHA_BRANCH_NEWS',
                  'CHA_BRANCH_ORGANIZE',
                  'CHA_BRANCH_SCHOOL',
                  'CHA_BRANCH_SUBWAY',
                  'CHA_BRANCH_WECHAT',
                  'CHA_BRANCH_WEIBO_INFO']
    for table in table_list:
        print(table)
        phoenix_curs.execute(
            f"select substr(CREATE_TIME_,0,10) as DATE_,ENTITY_CODE_,ENTITY_NAME_,count(1) from {table} group by substr(CREATE_TIME_,0,10),ENTITY_CODE_,ENTITY_NAME_")
        all_data = phoenix_curs.fetchall()
        for i in all_data:
            data = {}
            data['ID_']=i[1]+i[0]
            data['ENTITY_CODE_'] = i[1]
            data['ENTITY_NAME_'] = i[2]
            data['MONGO_COLLECTION_'] = ''
            data['HBASE_TABLE_'] = table
            data['FIX_URL_COUNT_'] = 0
            data['TEMP_URL_COUNT_'] = 0
            data['SPIDER_DATA_COUNT_'] = 0
            data['SHUFFLE_COUNT_'] = i[3]
            data['COMPARE_'] = 0
            data['DATE_'] = i[0]
            print(data)
            phoenix.upsert_to_phoenix_by_one(connection, data, table_name='CHA_DATA_MONITOR')
    connection.close()
