# -*- coding: utf-8 -*-
"""理财产品 融 360"""
import hashlib
from copy import deepcopy
from time import sleep

import pymongo
import re

from __config import *
from database._mongodb import MongoClient
from database._mysql import MysqlClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger


class ScriptCCB(object):
    def __init__(self):
        self.logger = Logger().logger
        self.remove_id_list = list()
        self.copy_mongo_data_list = list()
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

        self.sales_status = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'SALES_STATUS\'",
            connection=__mysql_connection)
        self.produc_category = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'PRODUC_CATEGORY\'",
            connection=__mysql_connection)
        self.revenue_type = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'REVENUE_TYPE\'",
            connection=__mysql_connection)
        self.operaton_pattern = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'OPERATION_PATTERN\'",
            connection=__mysql_connection)
        self.purchase_amount = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'PURCHASE_AMOUNT\'",
            connection=__mysql_connection)
        self.duration_type = __mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'DURATION_TYPE\'",
            connection=__mysql_connection)
        __mysql_client.close_client(connection=__mysql_connection)

        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.verify_list = ["ID_", "ENTITY_CODE_", "AREA_CODE_", "BANK_CODE_", "BANK_NAME_", "UNIT_CODE_",
                            "PERIOD_CODE_", "CONTENT_", "REMARK_", "CREATE_TIME_", "UPDATE_TIME_", "CODE_", "NAME_",
                            "TIME_LIMIT_", "YIELD_RATE_", "BREAKEVEN_", "START_FUNDS_", "INVEST_PERIOD_", "SALE_START_",
                            "SALE_END_", "RISK_LEVEL_", "REDEMING_MODE_", "PRIVATE_BANK_", "URL_", "DEALTIME_",
                            "DATETIME_", "ENTITY_NAME_", "STATUS_", "SALE_DISTRICT_", "CURRENCY_TYPE_",
                            "INCREASE_UNIT_", "YIELD_START_DATE_", "YIELD_END_DATE_", "YIELD_TYPE_", "TARGET_",
                            "PRODUCT_TYPE_", "YIELD_STATMENT_", "INVEST_RANGE_", "PRE_STOP_", "RASE_PLAN_", "PURCHASE_"]

    @staticmethod
    def get_data_from_mongo(self, m_client, collection, data_id):
        """
        :param m_client: MongoDB client
        :param collection: MongoDB collection
        :return: data from MongoDB
        """
        m_client.mongo_db = "spider_data"

        try:
            mongo_data_list = m_client.all_from_mongodb(collection, data_id=data_id)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            sleep(1)
            mongo_data_list = m_client.all_from_mongodb(collection, data_id=data_id)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None
        except KeyError as e:
            self.logger.info(e)
            return None

    @staticmethod
    def data_shuffle(self, data):
        if "上海银行" in data["BANK_NAME"]:
            bank_code = "BankOfShanghai"
        elif "天津银行" in data["BANK_NAME"]:
            bank_code = "TJBANK"
        elif "北京银行" in data["BANK_NAME"]:
            bank_code = "BOB"
        elif "中国光大银行股份有限公司" in data["BANK_NAME"]:
            bank_code = "CEB"
        else:
            print(data)

        re_data = dict()
        hash_m = hashlib.md5()
        hash_m.update(data["NAME_"].encode("utf-8"))
        hash_id = hash_m.hexdigest()
        re_data["ID_"] = bank_code + "_" + hash_id + "_" + data["SALE_START_"]
        re_data["ENTITY_CODE_"] = "RONG360FINANCIAL"
        # re_data["AREA_CODE_"]
        re_data["BANK_CODE_"] = bank_code
        re_data["BANK_NAME_"] = data["BANK_NAME"].replace("股份有限公司", "")
        # re_data["UNIT_CODE_"]
        re_data["PERIOD_CODE_"] = data["SALE_START_"].replace("-", "")
        # re_data["CONTENT_"]
        re_data["STATUS_"] = ""
        # re_data["REMARK_"]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        # re_data["UPDATE_TIME_"]

        re_data["NAME_"] = data["NAME_"]
        # 售卖时间范围
        re_data["TIME_LIMIT_"] = ""
        # 收益率
        re_data["LOW_YIELD_RATE_"] = data["YIELD_RATE_"].replace("%", "")
        re_data["HIGH_YIELD_RATE_"] = data["YIELD_RATE_"].replace("%", "")
        # 售卖区域
        re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
        # 起购金额
        data["START_FUNDS_"] = data["START_FUNDS_"].replace("亿", "00000000")
        data["START_FUNDS_"] = data["START_FUNDS_"].replace("万", "0000")
        data["START_FUNDS_"] = data["START_FUNDS_"].replace("千", "000")
        if data["START_FUNDS_"]:
            if int(data["START_FUNDS_"]) < 50000:
                match_funds = "5万以下"
            elif 50000 <= int(data["START_FUNDS_"]) < 100000:
                match_funds = "5万-10万"
            elif 100000 <= int(data["START_FUNDS_"]) < 200000:
                match_funds = "10万-20万"
            elif 20000 <= int(data["START_FUNDS_"]) < 500000:
                match_funds = "20万-50万"
            elif 500000 <= int(data["START_FUNDS_"]):
                match_funds = "50万以上"
        else:
            match_funds = "不限"
        for i in self.purchase_amount:
            if match_funds in i["ITEM_LABEL_"]:
                re_data["START_FUNDS_"] = data["START_FUNDS_"]
                re_data["START_FUNDS_CODE_"] = i["ITEM_VALUE_"]

        # 期限
        data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"].replace("天", "")

        if data["INVEST_PERIOD_"]:
            if int(data["INVEST_PERIOD_"]) <= 30:
                match_str = "1个月内"
            elif 30 < int(data["INVEST_PERIOD_"]) <= 90:
                match_str = "1-3个月（含）"
            elif 90 < int(data["INVEST_PERIOD_"]) <= 180:
                match_str = "3-6个月（含）"
            elif 180 < int(data["INVEST_PERIOD_"]) <= 365:
                match_str = "6-12个月（含）"
            elif 365 < int(data["INVEST_PERIOD_"]):
                match_str = "12个月以上"

        else:
            match_str = "不限"

        for i in self.duration_type:
            if match_str in i["ITEM_LABEL_"]:
                re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
                re_data["INVEST_PERIOD_CODE_"] = i["ITEM_VALUE_"]

        # 开始售卖时间
        re_data["SALE_START_"] = data["SALE_START_"]
        # 结束售卖时间
        re_data["SALE_END_"] = data["SALE_END_"]
        # 风险等级
        # re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
        re_data["URL_"] = data["URL_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["DATETIME_"] = data["DATETIME_"]
        re_data["ENTITY_NAME_"] = "融360理财产品"
        # NEW
        # 认购货币（类型）
        re_data["CURRENCY_TYPE_"] = data["CURRENCY_TYPE_"]
        # 递增单位
        re_data["INCREASE_UNIT_"] = re.sub(r"元.*", "", data["INCREASE_UNIT_"])
        # 收益起记（日期）
        re_data["YIELD_START_DATE_"] = data["YIELD_START_DATE_"][:10]
        # 收益结束（日期）
        re_data["YIELD_END_DATE_"] = data["YIELD_START_DATE_"][-10:]
        # 收益获取方式
        for i in self.revenue_type:
            if i["ITEM_LABEL_"] == data["YIELD_TYPE_"]:
                re_data["YIELD_TYPE_"] = data["YIELD_TYPE_"]
                re_data["YIELD_TYPE_CODE_"] = i["ITEM_VALUE_"]
                break
        # 对象(目标人群)
        re_data["TARGET_"] = data["TARGET_"]
        # 产品类型
        re_data["PRODUCT_TYPE_"] = data["PRODUCT_TYPE_"]

        # 收益率说明
        re_data["YIELD_STATMENT_"] = data["YIELD_STATMENT_"]

        # 投资范围
        re_data["INVEST_RANGE_"] = data["INVEST_RANGE_"]
        # 提前终止条件
        re_data["PRE_STOP_"] = data["PRE_STOP_"]
        # 募集规划条件
        re_data["RASE_PLAN_"] = data["RASE_PLAN_"]
        # 申购条件
        re_data["PURCHASE_"] = data["PURCHASE_"]
        # 无
        # re_data["CODE_"] = data["CODE_"]
        # 是否保本
        # re_data["BREAKEVEN_"] = data["BREAKEVEN_"]
        # # 可否赎回
        # re_data["REDEMING_MODE_"]
        # # 私人银行
        # re_data["PRIVATE_BANK_"]

        return re_data

    def run(self):
        # 创建 Phoenix 对象
        p_client = PhoenixHbase(table_name="FINPRODUCT_FINASSIST")
        p_client.verify_list = self.verify_list
        # 连接 Phoenix
        connection = p_client.connect_to_phoenix()

        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="JSFIN_CCBDATA")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)
        # 创建 MongoDB spider_data_old 数据库对象
        # old_client = MongoClient(mongo_collection="FINPRODUCT_FINASSIST")
        # 本地测试
        # old_client.client = pymongo.MongoClient(host="localhost", port=27017,
        #                                         serverSelectionTimeoutMS=60, connectTimeoutMS=60, connect=False)
        # old_client.mongo_db = "spider_data_old"
        # db_old, collection_list_old = old_client.client_to_mongodb()
        # collection_old = db_old["FINPRODUCT_FINASSIST"]

        # 删除表
        # p_client.drop_table_phoenix(connection=connection)

        # # # 表创建语句
        # sql = ('create table "FINPRODUCT_FINASSIST" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #        '"C"."AREA_CODE_" varchar, "C"."BANK_CODE_" varchar, "C"."BANK_NAME_" varchar, "C"."IMPORTANCE_" varchar,'
        #        '"C"."UNIT_CODE_" varchar, "C"."PERIOD_CODE_" varchar, "C"."REMARK_" varchar, "C"."SALE_STATUS_CODE_" varchar,'
        #        '"C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar, "T"."CONTENT_" varchar,'
        #        '"C"."CODE_" varchar, "C"."NAME_" varchar, "C"."TIME_LIMIT_" varchar, "C"."SALE_STATUS_" varchar,'
        #        '"C"."LOW_YIELD_RATE_" varchar, "C"."HIGH_YIELD_RATE_" varchar, "C"."BREAKEVEN_" varchar,'
        #        '"C"."START_FUNDS_" varchar, "C"."START_FUNDS_CODE_" varchar, "C"."INVEST_PERIOD_" varchar,'
        #        '"C"."INVEST_PERIOD_CODE_" varchar, "C"."SALE_DISTRICT_" varchar, "C"."SALE_START_" varchar,'
        #        '"C"."SALE_END_" varchar, "C"."RISK_LEVEL_" varchar, "C"."REDEMING_MODE_" varchar,'
        #        '"C"."PRIVATE_BANK_" varchar, "C"."URL_" varchar, "C"."DEALTIME_" varchar, "C"."DATETIME_" varchar,'
        #        '"C"."ENTITY_NAME_" varchar, "C"."CURRENCY_TYPE_" varchar, "C"."INCREASE_UNIT_" varchar,'
        #        '"C"."YIELD_START_DATE_" varchar, "C"."YIELD_END_DATE_" varchar, "C"."YIELD_TYPE_" varchar,'
        #        '"C"."YIELD_TYPE_CODE_" varchar, "C"."TARGET_" varchar, "C"."PRODUCT_TYPE_" varchar,'
        #        '"C"."INVESTOR_TYPE_" varchar, "C". "INVESTOR_TYPE_CODE_" varchar, "C"."YIELD_STATMENT_" varchar,'
        #        '"C"."OPERA_MODEL_CODE_" varchar, "C"."OPERA_MODEL_" varchar,'
        #        '"C"."INVEST_RANGE_" varchar, "C"."PRE_STOP_" varchar, "C"."RASE_PLAN_" varchar,'
        #        '"C"."PURCHASE_" varchar, "C"."STATUS_" varchar) IMMUTABLE_ROWS = true')
        #
        # # 创建表
        # p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 增加列
        # colum_list = ["CURRENCY_TYPE_", "INCREASE_UNIT_", "YIELD_START_DATE_", "YIELD_END_DATE_", "YIELD_TYPE_",
        #               "TARGET_", "PRODUCT_TYPE_", "YIELD_STATMENT_", "INVEST_RANGE_", "PRE_STOP_", "RASE_PLAN_",
        #               "PURCHASE_"]
        # p_client.add_column_phoenix(connection=connection, column=colum_list)

        status = False
        self.logger.info("开始进行 ENTITY_CODE_: RONG360FINANCIAL")
        self.remove_id_list = []
        self.copy_mongo_data_list = []
        find_id = None
        try:
            mongo_data_list = self.get_data_from_mongo(self=self, m_client=m_client, collection=collection, data_id=find_id)
        except pymongo.errors.ServerSelectionTimeoutError:
            sleep(1)
            mongo_data_list = self.get_data_from_mongo(self=self, m_client=m_client, collection=collection, data_id=find_id)

        # 清洗数据并插入 HBase
        if mongo_data_list:
            once_count = 0
            self.find_count += mongo_data_list.count()
            for data in mongo_data_list:
                data_id = data["_id"]
                copy_data = {}
                self.remove_id_list.append(data_id)
                try:
                    del data["_id"]
                    copy_data = deepcopy(data)
                    self.copy_mongo_data_list.append(copy_data)
                    re_data = self.data_shuffle(self=self, data=data)

                    if not re_data:
                        self.bad_count += 1
                        continue
                except Exception as e:
                    self.remove_id_list.remove(data_id)
                    self.copy_mongo_data_list.remove(data)
                    self.logger.warning("清洗错误,错误 _id 为{}, {}".format(data_id, e))
                    continue

                print(data_id)

                # phoenix_HBase 插入数据
                if isinstance(re_data, dict):
                    try:
                        success_count = p_client.upsert_to_phoenix_by_one(connection=connection, data=re_data)
                        once_count += success_count
                        self.success_count += success_count
                        # self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                    except Exception as e:
                        self.remove_id_list.remove(data_id)
                        self.copy_mongo_data_list.remove(data)
                        self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                        continue
                elif isinstance(re_data, list):
                    for r_data in re_data:
                        try:
                            success_count = p_client.upsert_to_phoenix_by_one(connection=connection,
                                                                              data=r_data)
                            once_count += success_count
                            self.success_count += success_count
                            # self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                        except Exception as e:
                            self.remove_id_list.remove(data_id)
                            self.copy_mongo_data_list.remove(data)
                            self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                            continue
            if once_count > 0:
                status = True
                self.logger.info("HBase 插入成功, 成功条数 {}".format(once_count))
        else:
            quit()
        # # 添加 {d:1}
        # if status:
        #     update_count = m_client.update_to_mongodb(collection=collection, data_id=self.remove_id_list,
        #                                               data_dict={"d": 1})
        #     self.remove_count += update_count
        #     self.logger.info("MongoDB 更新成功")
        # # 删除数据
        # if status:
        #     delete_count = self.delete_data_from_mongo(m_client=m_client, collection=collection,
        #                                                entity_code=entity,
        #                                                remove_id_list=self.remove_id_list)
        #     self.remove_count += delete_count
        #     # self.logger.info("MongoDB 删除成功")
        # else:
        #     self.logger.info("HBase 插入成功条数0条, 不执行删除")
        #
        # 将数据插入 spider_data_old 中
        # if status:
        #     try:
        #         old_client.mongo_db = "spider_data_old"
        #         insert_count = old_client.all_to_mongodb(collection=collection_old,
        #                                                  insert_list=self.copy_mongo_data_list)
        #         self.old_count += insert_count
        #         # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
        #     except pymongo.errors.ServerSelectionTimeoutError as e:
        #         sleep(1)
        #         self.logger.info("MongoDB 连接失败, 正在重新连接 {}".format(e))
        #         insert_count = old_client.all_to_mongodb(collection=collection_old,
        #                                                  insert_list=self.copy_mongo_data_list)
        #         self.old_count += insert_count
        #         # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
        #     except Exception as e:
        #         self.logger.info(e)

        # 关闭连接
        m_client.client_close()
        # p_client.close_client_phoenix(connection=connection)
        self.logger.info("本次共向 MongoDB 查取数据{}条".format(self.find_count))
        self.logger.info("本次共向 HBase 插入数据{}条".format(self.success_count))
        self.logger.info("本次共向 MongoDB 删除数据{}条".format(self.remove_count))
        self.logger.info("本次共向 MongoDB 插入数据{}条".format(self.old_count))
        self.logger.handlers.clear()


if __name__ == '__main__':
    script = ScriptCCB()
    script.run()
