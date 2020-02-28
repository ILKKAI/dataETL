# -*- coding: utf-8 -*-
"""华夏银行信用卡产品"""
import base64
import re
import uuid
import requests

from cha_creditcard.common import mongo_conn, mysql_conn, data_from_mongo, mysql_cursor, sql_edit
from cha_creditcard.config import *

COMMON_DICT = lambda: dict(
    DEAL_STATUS_="DRAFT",
    PUBLISH_STATUS_="N",
    BANK_CODE_="HXBANK",  # 银行编码
    BANK_NAME_="华夏银行",  # 银行名称
    CREATED_BY_ID_=CREATE_BY_ID,  # 创建人ID
    CREATED_BY_NAME_=CREATE_BY_NAME,  # 创建人名称
    CREATED_TIME_=CREATE_TIME(),  # 创建时间
    MODIFIED_TIME_=CREATE_TIME()  # 修改时间
)

# Mongo 集合配置
MONGO_CONFIG["collection"] = "XYK_CP"
# MySQL 表名配置
MYSQL_CONFIG["table_name"] = "cha_creditcard_pro"
# MYSQL_CONFIG["table_name"] = "cha_creditcard_pro_copy1"
MYSQL_CONFIG["img_table_name"] = "cha_creditcard_surface"
# MYSQL_CONFIG["img_table_name"] = "cha_creditcard_surface_copy1"

COUNT = 0


def init_client():
    """
    初始化
    :return:
    """
    global collection
    collection = mongo_conn()
    global mysql_client
    mysql_client = mysql_conn()

    aggre_for_name()


def aggre_for_name():
    agg_data = collection.aggregate([
        {"$match": {"ENTITY_CODE_": "XYK_CP_HXYH"}},
        {"$project": {"_id": 0, "CARD_NAME_": 1}},
        {"$group": {"_id": "$CARD_NAME_"}}
    ])

    global name_list
    name_list = [each["_id"] for each in agg_data]
    name_list = list(set(name_list))


def disting_table(data_iter):
    count = 0
    data_list = list()
    for data in data_iter:
        re_list = shuffle_data(data, count)
        data_list.extend(re_list)
        count += 1
    return data_list


def shuffle_data(data, count):
    re_list = list()
    if count == 0:
        re_data = dict()
        re_data.update(COMMON_DICT())

        re_data["ID_"] = str(uuid.uuid1())  # 主键
        global card_id
        card_id = re_data["ID_"]
        # re_data["VERSION_"] = ""  # 版本
        # re_data["CODE_"] = ""  # 编码
        re_data["NAME_"] = data.get('NAME_') if data.get('NAME_') else data['CARD_NAME_']  # 名称
        global card_name
        card_name = re_data["NAME_"]
        re_data["NEW_SALE_"] = "N"  # 最新
        re_data["GOOD_SALE_"] = "N"  # 畅销
        re_data["RECOMMEND_"] = "N"  # 主推
        re_list.append((MYSQL_CONFIG["table_name"], re_data))

    surface_dict = dict(COMMON_DICT())
    surface_dict["ID_"] = str(uuid.uuid1())  # 主键
    surface_dict["CARD_CODE_"] = card_id  # 信用卡编码
    surface_dict["CARD_NAME_"] = card_name  # 信用卡名称
    img_url = data["IMG_"]
    img_response = requests.get(url=img_url)
    surface_dict["IMAGE_"] = base64.b64encode(img_response.content).decode("utf-8")  # 卡面图片
    img_response.close()

    surface_dict["IS_LETTER_"] = "N"  # 卡面字母
    surface_dict["IS_CHINESE_"] = "N"  # 卡面汉字
    surface_dict["IS_SYMBOL_"] = "N"  # 卡面图形符号
    surface_dict["JOINTLY_"] = "N"  # 是否为联名卡

    surface_dict["NAME_"] = data["CARD_NAME_"]  # 卡面名称

    re_list.append((MYSQL_CONFIG["img_table_name"], surface_dict))
    return re_list


def main():
    """
    主函数
    :return:
    """
    init_client()
    for name in name_list:
        print(name)
        data_iter = data_from_mongo(find_query={"$and": [
            {"ENTITY_CODE_": "XYK_CP_HXYH"},
            {"CARD_NAME_": name}
        ]}, collection=collection)
        data_list = disting_table(data_iter)
        sql_list = list()
        for i in data_list:
            sql = sql_edit(i[1], table_name=i[0])
            sql_list.append(sql)
        global COUNT
        if len(sql_list) > 5:
            for sql in sql_list:
                c = mysql_cursor(sql=sql, client=mysql_client)
                COUNT += c
        else:
            c = mysql_cursor(sql=sql_list, client=mysql_client)
            COUNT += c
        print(COUNT)


if __name__ == '__main__':
    main()
