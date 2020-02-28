# -*- coding: utf-8 -*-
import pymongo
import pymysql

from cha_creditcard.config import MYSQL_CONFIG, MONGO_CONFIG


def mysql_conn():
    return pymysql.Connection(**MYSQL_CONFIG["config"])


def mysql_cursor(sql, client):
    # print(sql)
    """
    实例化 MySQL 游标并执行 SQL
    :param sql:
    :return:
    """
    count = 0

    cs = client.cursor()
    if isinstance(sql, str):
        sql = [sql]

    for s in sql:
        c = cs.execute(s)
        count += c
    client.commit()
    cs.close()
    return count


def sql_edit(data, table_name):
    """
    编辑 SQL 语句
    :param data:
    :param table_name:
    :return:
    """
    key_list = list()
    value_list = list()

    for key, value in data.items():
        key_list.append(key)
        if isinstance(value, str):
            value_list.append("\"" + value + "\"")
        else:
            value_list.append(value)

    key_sql = str(tuple(key_list)).replace("'", "")
    value_sql = str(tuple(value_list)).replace("'", "")

    sql = f"INSERT INTO {table_name} {key_sql} values {value_sql}"
    return sql


def mongo_conn():
    client = pymongo.MongoClient(host=MONGO_CONFIG["host"], port=MONGO_CONFIG["port"])
    db = client[MONGO_CONFIG["database"]]
    return db[MONGO_CONFIG["collection"]]


def data_from_mongo(find_query, collection):
    """
    获取数据
    :param find_query:
    :param collection:
    :return:
    """
    data_iter = collection.find(find_query).limit(1)
    if data_iter.count() == 0:
        print("无数据")
        quit()
    else:
        return data_iter


def write_in_text(content):
    with open("xxx.txt", "a", encoding="utf-8")as f:
        f.write(content)


if __name__ == '__main__':
    verify_list = list()
    MONGO_CONFIG["collection"] = "XYK_CP"
    collection = mongo_conn()
    result = collection.aggregate([
        # {"$match": {"$and": [{"ENTITY_CODE_": "XYK_CP_ZSYH"}]}},
        {"$match": {"$and": [
            {"ENTITY_CODE_": "XYK_CP_ZSYH"},
            {"URL_": {"$regex": "/ccard/"}}
            # {"URL_": {"$regex": "^((?!/ccard/).)*$"}}
                             ]}},
        {"$project": {"_id": 0, "NAME_": 1, "URL_": 1}},
        {"$group": {"_id": "$NAME_", "COUNT": {"$sum": 1}}}
                                   ])
    # print(result)
    count = 0
    for each in result:
        print(each)
        count += 1
        print(count)
        verify_list.append(each["_id"])

    result2 = collection.aggregate([
        # {"$match": {"$and": [{"ENTITY_CODE_": "XYK_CP_ZSYH"}]}},
        {"$match": {"$and": [
            {"ENTITY_CODE_": "XYK_CP_ZSYH"},
            # {"URL_": {"$regex": "^((?!/ccard/).)*$"}}
            {"URL_": {"$regex": "^((?!/ccard/).)*$"}}
        ]}},
        {"$project": {"_id": 0, "NAME_": 1, "URL_": 1}},
        {"$group": {"_id": "$NAME_", "URL_": {"$push": "$URL_"}, "COUNT": {"$sum": 1}}}
    ])
    # print(result2)
    count = 0
    for each in result2:
        if each["_id"] not in verify_list:
            print(each)
            count += 1
            print(count)
    print(len(set(verify_list)))
