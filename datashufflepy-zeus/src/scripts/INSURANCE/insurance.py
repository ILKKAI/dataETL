# -*- coding: utf-8 -*-
"""保险"""
import hashlib
import random
import re

import jaydebeapi
import pymongo
import time

from database._mongodb import MongoClient
from database._mysql import MysqlClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger
from __config import *


class JsInsuranceCcbData(object):
    def __init__(self):
        # 创建 MongoDB 对象
        self.m_client = MongoClient(mongo_collection="JSINSURANCE_CCBDATA")
        db, collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=db, collection_list=collection_list)

        # 创建 MySQL 对象
        __mysql_config = {
            "host": MYSQL_HOST_25,
            "port": MYSQL_PORT_25,
            "database": MYSQL_DATABASE_25,
            "user": MYSQL_USER_25,
            "password": MYSQL_PASSWORD_25,
            "table": MYSQL_TABLE_25
        }

        __mysql_client = MysqlClient(**__mysql_config)
        __mysql_connection = __mysql_client.client_to_mysql()

        self.type = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'TYPE\'",
            connection=__mysql_connection)

        __mysql_client.close_client(connection=__mysql_connection)

        # 创建 Phoenix 对象
        self.p_client = PhoenixHbase(table_name="INSURANCE")
        # 连接 Phoenix
        self.connection = self.p_client.connect_to_phoenix()

        self.logger = Logger().logger

        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.error_count = 0
        self.data_id = ""
        self.a = list()

    def data_shuffle(self, data):
        if data["ENTITY_CODE_"] == "PAINSURANCE":
            return None
        elif data["ENTITY_CODE_"] == "BJBINSURANCE":
            data["CONTET_"] = data["CONTET_"].replace("|主险2：", "主险2：")
            first_shuffle = data["CONTET_"].split("|")
            data_list = list()
            company_dict = dict()
            index_list = list()
            for first in first_shuffle:
                if first[-2:] == "公司":
                    company_index = first_shuffle.index(first)
                    company_dict[company_index] = first
                    index_list.append(company_index)
                else:
                    continue

            for key in index_list:
                # print(index_list)
                j = key + 1
                for i in range(100):
                    if index_list.index(key) == len(index_list) - 1:
                        if j == len(first_shuffle) - 1:
                            break
                    else:
                        if j == index_list[index_list.index(key) + 1]:
                            break

                    data_dict = dict()
                    # HBase row_key
                    hash_m = hashlib.md5()
                    hash_m.update(first_shuffle[j].encode("utf-8"))
                    hash_title = hash_m.hexdigest()
                    row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

                    # "C"
                    data_dict["ID_"] = row_key
                    data_dict["ENTITY_CODE_"] = data["ENTITY_CODE_"]
                    data_dict["ENTITY_NAME_"] = data["ENTITY_NAME_"].replace("模板", "产品")
                    data_dict["BANK_CODE_"] = "BJB"
                    data_dict["BANK_NAME_"] = "北京银行"
                    data_dict["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
                    data_dict["URL_"] = data["URL_"]
                    data_dict["PRODUCT_NAME_"] = first_shuffle[j]
                    j += 1
                    # data_dict["TYPE_"] = first_shuffle[j]
                    data_dict["TYPE_"] = ""
                    data_dict["TYPE_CODE_"] = ""
                    for i in self.type:
                        if i["ITEM_LABEL_"][:-1] in first_shuffle[j]:
                            data_dict["TYPE_"] = data_dict["TYPE_"] + i["ITEM_LABEL_"] + "|"
                            data_dict["TYPE_CODE_"] = data_dict["TYPE_CODE_"] + i["ITEM_VALUE_"] + "|"
                    data_dict["TYPE_"] = data_dict["TYPE_"][:-1]
                    data_dict["TYPE_CODE_"] = data_dict["TYPE_CODE_"][:-1]
                    j += 1
                    # data_dict["RISK_LEVEL_"] = first_shuffle[j]
                    j += 1
                    data_dict["PAY_METHOD_"] = first_shuffle[j]
                    j += 1
                    # data_dict["INSURANCE_DATE_"] = first_shuffle[j]
                    j += 1
                    # data_dict["TOUZIZHE_TYPE_"] = first_shuffle[j]
                    j += 1
                    data_dict["COM_NAME_"] = company_dict[key]
                    # data_dict["CONSIGNMENT_"] = "代销"
                    # if "CONTENT_" in data:
                    #     data_dict["CONTENT_"] = data["CONTENT_"]
                    data_dict["DEALTIME_"] = data["DEALTIME_"]
                    data_dict["CREATE_TIME_"] = data["DATETIME_"]
                    data_dict["STATUS_"] = "1"
                    # print(data_dict)
                    data_list.append(data_dict)

            return data_list

        elif data["ENTITY_CODE_"] == "CIBINSURANCE":
            data_list = list()
            insurance_name = re.findall(r".*?计划", data["PRODUCT_NAME_"])
            for name in insurance_name:
                re_data = dict()
                # HBase row_key
                hash_m = hashlib.md5()
                hash_m.update(name.encode("utf-8"))
                hash_title = hash_m.hexdigest()
                row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

                # "C"
                re_data["ID_"] = row_key
                re_data["PRODUCT_NAME_"] = name
                re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
                re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
                re_data["BANK_CODE_"] = "CIB"
                re_data["BANK_NAME_"] = "兴业银行"
                re_data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
                re_data["URL_"] = data["URL_"]
                re_data["DEALTIME_"] = data["DEALTIME_"]
                re_data["CREATE_TIME_"] = data["DATETIME_"]
                re_data["STATUS_"] = "1"

                re_data["TYPE_"] = ""
                re_data["TYPE_CODE_"] = ""
                for i in self.type:
                    if i["ITEM_LABEL_"][:-1] in name:
                        re_data["TYPE_"] = re_data["TYPE_"] + i["ITEM_LABEL_"] + "|"
                        re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"] + i["ITEM_VALUE_"] + "|"
                re_data["TYPE_"] = re_data["TYPE_"][:-1]
                re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"][:-1]
                data_list.append(re_data)

            return data_list

        else:
            if "INSURANCE_NAME_" not in data and ("PRODUCT_NAME_" not in data):
                return None
            else:
                if "INSURANCE_NAME_" in data:
                    # # 承保年龄
                    # if ("INSURANCE_AGE_" not in data) or (not data["INSURANCE_AGE_"]):
                    #     age = re.findall(r"(\d*)周岁", data["INSURANCE_NAME_"])
                    #     if age:
                    #         data["INSURANCE_AGE_"] = age[0]

                    # 保障期限
                    # if ("INSURANCE_DATE_" not in data) or (not data["INSURANCE_DATE_"]):
                    #     limit = re.findall(r"保(终身)|保(\d*年)|(\d*年)期", data["INSURANCE_NAME_"])
                    #     if limit:
                    #         for l in limit[0]:
                    #             if l:
                    #                 data["INSURANCE_DATE_"] = l
                    #                 break

                    re_data = dict()
                    # HBase row_key
                    hash_m = hashlib.md5()
                    hash_m.update(data["INSURANCE_NAME_"].encode("utf-8"))
                    hash_title = hash_m.hexdigest()
                    row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

                    # "C"
                    re_data["ID_"] = row_key
                    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
                    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
                    re_data["BANK_CODE_"] = data["ENTITY_CODE_"].replace("INSURANCE", "")
                    re_data["BANK_NAME_"] = data["ENTITY_NAME_"].replace("保险产品", "")
                    if "INSURANCE_NAME_" in data:
                        re_data["PRODUCT_NAME_"] = data["INSURANCE_NAME_"]
                    if ("INSURANCE_AGE_" in data) or ("AGE_" in data):
                        re_data["AGE_"] = data["INSURANCE_AGE_"]
                    if "TYPE_" in data:
                        re_data["TYPE_"] = ""
                        re_data["TYPE_CODE_"] = ""
                        if data["TYPE_"] == "财险":
                            re_data["TYPE_"] = "财产险"
                            re_data["TYPE_CODE_"] = "PROPERTY_INSURANCE"
                        else:
                            for i in self.type:
                                if i["ITEM_LABEL_"][:-1] in data["TYPE_"]:
                                    re_data["TYPE_"] = re_data["TYPE_"] + i["ITEM_LABEL_"] + "|"
                                    re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"] + i["ITEM_VALUE_"] + "|"
                            re_data["TYPE_"] = re_data["TYPE_"][:-1]
                            re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"][:-1]
                    else:
                        re_data["TYPE_"] = ""
                        re_data["TYPE_CODE_"] = ""
                        for i in self.type:
                            if i["ITEM_LABEL_"][:-1] in data["ENTITY_NAME_"]:
                                re_data["TYPE_"] = re_data["TYPE_"] + i["ITEM_LABEL_"] + "|"
                                re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"] + i["ITEM_VALUE_"] + "|"
                        re_data["TYPE_"] = re_data["TYPE_"][:-1]
                        re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"][:-1]
                    # if "INSURANCE_DATE_" in data:
                    #     re_data["INSURANCE_DATE_"] = data["INSURANCE_DATE_"]
                    if "INSURANCE_DETAIL_" in data:
                        re_data["PRODUCT_DETAIL_"] = data["INSURANCE_DETAIL_"]
                    if "COMPANY_NAME_" in data:
                        re_data["COM_NAME_"] = data["COMPANY_NAME_"]
                    if "LIMIT_NUMBER_" in data:
                        re_data["BUY_LIMIT_"] = data["LIMIT_NUMBER_"]
                    # re_data["AREA_CODE_"]
                    # re_data["UNIT_CODE_"]
                    re_data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
                    if "CONTENT_" in data:
                        re_data["CONTENT_"] = data["CONTENT_"]
                    # re_data["NOTICE_TIME_"] = data["NOTICE_TIME_"]
                    re_data["STATUS_"] = "1"
                    # re_data["REMARK_"] = ""
                    re_data["CREATE_TIME_"] = data["DATETIME_"]
                    # re_data["UPDATE_TIME_"]
                    # re_data["TITLE_"] = data["TITLE_"]
                    re_data["URL_"] = data["URL_"]
                    re_data["DEALTIME_"] = data["DEALTIME_"]
                    # re_data["DATETIME_"] = data["DATETIME_"]

                    return re_data

                elif "PRODUCT_NAME_" in data:
                    re_data = dict()
                    # HBase row_key
                    hash_m = hashlib.md5()
                    hash_m.update(data["PRODUCT_NAME_"].encode("utf-8"))
                    hash_title = hash_m.hexdigest()
                    row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

                    # "C"
                    re_data["ID_"] = row_key
                    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
                    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
                    # re_data["BANK_CODE_"] = data["ENTITY_CODE_"]
                    # re_data["BANK_NAME_"] = data["ENTITY_NAME_"]
                    if "PRODUCT_NAME_" in data:
                        re_data["PRODUCT_NAME_"] = data["PRODUCT_NAME_"]
                    if "FEATURE_NAME_" in data:
                        re_data["FEATURE_NAME_"] = data["FEATURE_NAME_"]
                    if "TYPE_" in data:
                        re_data["TYPE_"] = ""
                        re_data["TYPE_CODE_"] = ""
                        if data["TYPE_"] == "财险":
                            re_data["TYPE_"] = "财产险"
                            re_data["TYPE_CODE_"] = "PROPERTY_INSURANCE"
                        elif data["TYPE_"] == "100种疾病保障":
                            re_data["TYPE_"] = "健康险"
                            re_data["TYPE_CODE_"] = "HEALTH_INSURANCE"
                        else:
                            for i in self.type:
                                if i["ITEM_LABEL_"][:-1] in data["TYPE_"]:
                                    re_data["TYPE_"] = re_data["TYPE_"] + i["ITEM_LABEL_"] + "|"
                                    re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"] + i["ITEM_VALUE_"] + "|"
                            re_data["TYPE_"] = re_data["TYPE_"][:-1]
                            re_data["TYPE_CODE_"] = re_data["TYPE_CODE_"][:-1]
                    if "POLICY_DUTY_" in data:
                        re_data["POLICY_DUTY_"] = data["POLICY_DUTY_"]
                    if "PRODUCT_CASE_" in data:
                        re_data["PRODUCT_CASE_"] = data["PRODUCT_CASE_"]
                    if "BUY_LIMIT_" in data:
                        re_data["BUY_LIMIT_"] = data["BUY_LIMIT_"]
                    if "ENSURE_PRICE_" in data:
                        re_data["ENSURE_PRICE_"] = data["ENSURE_PRICE_"]
                    # re_data["AREA_CODE_"]
                    # re_data["UNIT_CODE_"]
                    re_data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
                    if "PRODUCT_PRICE_" in data:
                        re_data["PRODUCT_PRICE_"] = data["PRODUCT_PRICE_"]
                    if "PRODUCT_ID_" in data:
                        re_data["PRODUCT_ID_"] = data["PRODUCT_ID_"]
                    if "PRODUCT_CLAUSE_" in data:
                        re_data["PRODUCT_CLAUSE_"] = data["PRODUCT_CLAUSE_"]
                    if "GENDER_" in data:
                        re_data["GENDER_"] = data["GENDER_"]
                    if "AGE_" in data:
                        re_data["AGE_"] = data["AGE_"]
                    if "COM_NAME_" in data:
                        re_data["COM_NAME_"] = data["COM_NAME_"]
                    if "PAY_METHOD_" in data:
                        re_data["PAY_METHOD_"] = data["PAY_METHOD_"]
                    if "PROBLEM_" in data:
                        re_data["PROBLEM_"] = data["PROBLEM_"]
                    if "CLAIM_" in data:
                        re_data["CLAIM_"] = data["CLAIM_"]
                    if "COMMENT_" in data:
                        re_data["COMMENT_"] = data["COMMENT_"]
                    if "ENSURE_CONTENT_" in data:
                        re_data["ENSURE_CONTENT_"] = data["ENSURE_CONTENT_"]
                    if "INSURE_INFO_" in data:
                        re_data["INSURE_INFO_"] = data["INSURE_INFO_"]
                    if "RATE_INFO_" in data:
                        re_data["RATE_INFO_"] = data["RATE_INFO_"]
                    if "SALE_SERVICE_" in data:
                        re_data["SALE_SERVICE_"] = data["SALE_SERVICE_"]

                    # re_data["NOTICE_TIME_"] = data["NOTICE_TIME_"]
                    re_data["STATUS_"] = "1"
                    # re_data["REMARK_"] = ""
                    re_data["CREATE_TIME_"] = data["DATETIME_"]
                    # re_data["UPDATE_TIME_"]
                    # re_data["TITLE_"] = data["TITLE_"]
                    re_data["URL_"] = data["URL_"]
                    re_data["DEALTIME_"] = data["DEALTIME_"]
                    # re_data["DATETIME_"] = data["DATETIME_"]

                    return re_data

    def run(self):
        # # delete table
        # self.p_client.drop_table_phoenix(connection=self.connection)
        # # quit()
        #
        # # create table sql
        # table_sql = ('create table "INSURANCE" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #              '"C"."ENTITY_NAME_" varchar, "C"."AREA_CODE_" varchar,"C"."BANK_CODE_" varchar,'
        #              ' "C"."BANK_NAME_" varchar, "C"."UNIT_CODE_" varchar, "C"."PERIOD_CODE_" varchar, '
        #              '"C"."REMARK_" varchar, "C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar,'
        #              '"C"."TYPE_" varchar, "C"."URL_" varchar, "C"."DEALTIME_" varchar, "C".PRODUCT_CLAUSE_ varchar,'
        #              '"C"."SOURCE_" varchar, "C"."PRODUCT_NAME_" varchar, "C"."FEATURE_NAME_" varchar,'
        #              '"C"."POLICY_DUTY_" varchar, "C"."PRODUCT_CASE_" varchar, "C"."BUY_LIMIT_" varchar,'
        #              '"C"."ENSURE_PRICE_" varchar, "C"."PRODUCT_PRICE_" varchar, "C"."PRODUCT_ID_" varchar,'
        #              '"C"."GENDER_" varchar, "C"."AGE_" varchar, "C"."COM_NAME_" varchar, "C"."TYPE_CODE_" varchar,'
        #              '"C"."PAY_METHOD_" varchar, "C"."PRODUCT_DETAIL_" varchar, "C"."PROBLEM_" varchar,'
        #              '"C"."CLAIM_" varchar, "C"."COMMENT_" varchar, "C"."STATUS_" varchar,'
        #              '"C"."ENSURE_CONTENT_" varchar, "C"."INSURE_INFO_" varchar, "C"."RATE_INFO_" varchar,'
        #              '"C"."SALE_SERVICE_" varchar) IMMUTABLE_ROWS = true')
        #
        # # create table
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection)

        for i in range(mongo_data_list.count() + 100):
            try:
                data = mongo_data_list.__next__()
            except StopIteration:
                break
            except pymongo.errors.ServerSelectionTimeoutError as e:
                self.logger.info("MongoDB 超时, 正在重新连接, 错误信息 {}".format(e))
                time.sleep(3)
                data = mongo_data_list.__next__()

            self.data_id = data["_id"]
            if self.success_count % 100 == 0:
                self.logger.info("正在进行 _id 为 {} 的数据".format(self.data_id))
            # print(data["_id"])
            # todo remove and upsert data from mongo

            # shuffle data
            try:
                re_data = self.data_shuffle(data=data)
            except Exception as e:
                self.logger.info("数据清洗失败 {}, id: {}".format(e, self.data_id))
                continue

            if re_data:
                if isinstance(re_data, dict):
                    # upsert data to HBase
                    try:
                        success_count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=re_data)
                    except jaydebeapi.DatabaseError as e:
                        self.logger.info("错误 id: {}, 错误信息 {}".format(self.data_id, e))
                        continue

                elif isinstance(re_data, list):
                    for r_d in re_data:
                        # upsert data to HBase
                        try:
                            success_count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection,
                                                                                   data=r_d)
                        except jaydebeapi.DatabaseError as e:
                            self.logger.info("错误 id: {}, 错误信息 {}".format(self.data_id, e))
                            continue

            #     # add {d:1}
            #     try:
            #         self.m_client.update_to_mongodb(collection=self.collection, data_id=self.data_id,
            #                                         data_dict={"d": 1})
            #         self.remove_count += 1
            #         if self.remove_count % 10 == 0:
            #             self.logger.info("MongoDB 更新成功, 成功条数 {}".format(self.remove_count))
            #     except Exception as e:
            #         self.logger.info("MongoDB 更新 _id 为 {} 的数据失败, {}".format(self.data_id, e))
            #         continue
                if success_count > 0:
                    status = True
                    self.success_count += success_count

                if self.success_count % 10 == 0:
                    self.logger.info("HBase 插入成功 {} 条".format(self.success_count))

            else:
                self.bad_count += 1
                continue

        mongo_data_list.close()

        self.logger.info("本次共向 MongoDB 查取数据{}条".format(self.find_count))
        self.logger.info("本次共向 HBase 插入数据{}条".format(self.success_count))
        self.logger.info("本次共向 MongoDB 删除数据{}条".format(self.remove_count))
        self.logger.info("本次共向 MongoDB 插入数据{}条".format(self.old_count))
        self.logger.info("本次坏数据共 {} 条".format(self.bad_count))
        self.logger.handlers.clear()


if __name__ == '__main__':
    script = JsInsuranceCcbData()
    script.run()
