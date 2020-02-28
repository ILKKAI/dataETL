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

sender = '1059792930@qq.com'
authCode = 'kctabxyfjkvzbbbc'


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
    table_list = []
    with open("./monitor_basic.json", encoding="utf-8") as f:
        load_dict = json.load(f)
        table_list = load_dict['RECORDS']
    time_array = time.localtime()
    create_time = time.strftime("%Y-%m-%d", time_array)
    create_time='2019-08-29'
    end_date = get_day(create_time, 1)
    print(create_time)
    print(end_date)
    for table in table_list:
        print(table)
        data = {}
        if table['ENTITY_CODE_']:
            phoenix_curs.execute(f"select count(1) from {table['HBASE_TABLE_']} "
                                 f"where ENTITY_CODE_='{table['ENTITY_CODE_']}' and CREATE_TIME_>'{create_time}'"
                                 f" and CREATE_TIME_<'{end_date}'")
            data['ID_'] = table['ENTITY_CODE_'] + create_time
        else:
            phoenix_curs.execute(
                f"select count(1) from {table['HBASE_TABLE_']} where CREATE_TIME_>'{create_time}' and CREATE_TIME_<'{end_date}'")
            data['ID_'] = table['HBASE_TABLE_'] + create_time
        count = phoenix_curs.fetchone()
        print(count[0].value)
        print("=" * 10)
        data['ENTITY_CODE_'] = table['ENTITY_CODE_']
        data['ENTITY_NAME_'] = table['ENTITY_NAME_']
        data['MONGO_COLLECTION_'] = table['MONGO_COLLECTION_']
        data['HBASE_TABLE_'] = table['HBASE_TABLE_']
        data['FIX_URL_COUNT_'] = 0
        data['TEMP_URL_COUNT_'] = 0
        data['SPIDER_DATA_COUNT_'] = 0
        data['SHUFFLE_COUNT_'] = count[0].value
        data['COMPARE_'] = 0
        data['DATE_'] = create_time
        phoenix.upsert_to_phoenix_by_one(connection, data, table_name='CHA_DATA_MONITOR')
    connection.close()
