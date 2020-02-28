# -*- coding: utf-8 -*-
import uuid

from database._mysql import MysqlClient
from database._phoenix_hbase import PhoenixHbase
from __config import *


def mysql_connect():
    mysql_config = {
        "host": MYSQL_HOST_25,
        "port": MYSQL_PORT_25,
        "database": MYSQL_DATABASE_25,
        "user": MYSQL_USER_25,
        "password": MYSQL_PASSWORD_25,
        "table": "cha_network_volume"
    }

    mysql_client = MysqlClient(**mysql_config)
    connection = mysql_client.client_to_mysql()

    return mysql_client, connection


def count_network_volume(data):
    insert_list = list()
    source_dict = {"微信": "WECHAT", "微博": "WEIBO", "CJXW": "FINANCE", "BDXW": "LOCALNEWS", "GWDT": "OFFICIAL"}
    bank_list = data["BANK_CODE_"].split("|")
    # 微信 WECHAT、微博 WEIBO、财经 FINANCE、本地新闻 LOCALNEWS、官网动态 OFFICIAL
    source = source_dict[data["SOURCE_TYPE_"]]
    publish_time = data["PUBLISH_TIME_"]
    source_id = data["ID_"]
    title = data["TITLE_"]

    for bank in bank_list:
        bank_code = bank
        # 保险 INSURANCE、基金 FUND、理财 FINANCING、信用卡 CREDIT
        for each in ["保险", "基金", "理财", "信用卡"]:
            insert_data = dict()
            type_ = each
            count = data["CONTENT_"].count(each)
            if count == 0:
                continue
            else:
                insert_data["ID_"] = str(uuid.uuid1())
                insert_data["SOURCE_"] = source
                insert_data["PUBLISH_TIME_"] = publish_time
                insert_data["SOURCE_ID_"] = source_id
                insert_data["TITLE_"] = title
                insert_data["BANK_CODE_"] = bank_code
                insert_data["TYPE_"] = type_
                insert_data["COUNT_"] = count
                insert_list.append(insert_data)

        if insert_list:
            # mysql_client.mysql_table = "cha_network_volume"
            mysql_client.insert_to_mysql(connection=mysql_connection, data=insert_list)


mysql_client, mysql_connection = mysql_connect()
p_client = PhoenixHbase(table_name="CHA_BRANCH_NEWS")
connection = p_client.connect_to_phoenix()
# 返回生成器对象
result_generator = p_client.search_all_from_phoenix(connection=connection, dict_status=True)

while True:
    try:
        result = result_generator.__next__()
        count_network_volume(data=result)
        # p_client.upsert_to_phoenix_by_one(connection=connection, data=result)

    except StopIteration:
        break
    except Exception as e:
        print(e)
        continue

connection.close()
