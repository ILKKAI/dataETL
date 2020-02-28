# -*- coding: utf-8 -*-
"""理财产品 中国理财网"""
from copy import deepcopy
from time import sleep

import pymongo
from bson.objectid import ObjectId

from database._mongodb import MongoClient
from database._mysql import MysqlClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger
from __config import *


class FinProductScript(object):
    def __init__(self):
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

        self.logger = Logger().logger
        self.remove_id_list = list()
        self.copy_mongo_data_list = list()
        # "CZBFinancial", "PABFinancial", "PSBCFinancial",
        #
        self.entity_list = ["ABCFinancial", "BOCFinancial", "BOCOMFinancial", "CBHBFinancial", "CCBFinancial",
                            "CEBFinancial", "CGBFinancial", "CIBFinancial", "CMBCFinancial", "CMBFinancial",
                            "EBCLFinancial", "ECITICFinancial", "HXBFinancial", "ICBCFinancial",
                            "SPDBFinancial", "CHINANETFINANCIAL", "JSFIN_CCBDATA"]

        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.verify_list = ["ID_", "ENTITY_CODE_", "AREA_CODE_", "BANK_CODE_", "BANK_NAME_", "UNIT_CODE_",
                            "PERIOD_CODE_", "CONTENT_", "REMARK_", "CREATE_TIME_", "UPDATE_TIME_", "CODE_", "NAME_",
                            "TIME_LIMIT_", "YIELD_RATE_", "BREAKEVEN_", "START_FUNDS_", "INVEST_PERIOD_", "SALE_START_",
                            "SALE_END_", "RISK_LEVEL_", "REDEMING_MODE_", "PRIVATE_BANK_", "URL_", "DEALTIME_",
                            "DATETIME_", "ENTITY_NAME_", "STATUS_", "SALE_DISTRICT_"]

    def get_data_from_mongo(self, m_client, collection, entity_code, data_id):
        """
        :param m_client: MongoDB client
        :param collection: MongoDB collection
        :param entity_code:
        :return: data from MongoDB
        """
        m_client.mongo_db = "spider_data"
        m_client.mongo_entity_code = entity_code

        try:
            mongo_data_list = m_client.search_from_mongodb(collection, data_id=data_id)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            sleep(1)
            mongo_data_list = m_client.search_from_mongodb(collection, data_id=data_id)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None
        except KeyError as e:
            self.logger.info(e)
            return None

    def delete_data_from_mongo(self, m_client, collection, entity_code, remove_id_list):
        """
        :param m_client: MongoDB client
        :param collection: MongoDB collection
        :param entity_code:
        :param remove_id_list: id list to remove
        :return: delete count
        """
        m_client.mongo_entity_code = entity_code

        try:
            remove_count = m_client.remove_from_mongo(collection=collection, remove_id_list=remove_id_list)
            return remove_count
        except pymongo.errors.ServerSelectionTimeoutError:
            mongo_data_list = m_client.remove_from_mongo(collection=collection, remove_id_list=remove_id_list)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None
        except KeyError as e:
            self.logger.info(e)
            return None

    def run(self):
        # 创建 Phoenix 对象
        p_client = PhoenixHbase(table_name="FINPRODUCT_FINASSIST")
        p_client.verify_list = self.verify_list
        # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="FINPRODUCT_FINASSIST")
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

        # # 删除表
        # p_client.drop_table_phoenix(connection=connection)

        # # 表创建语句
        # sql = ('create table "FINPRODUCT_FINASSIST" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #        '"C"."AREA_CODE_" varchar,"C"."BANK_CODE_" varchar,"C"."BANK_NAME_" varchar,'
        #        '"C"."UNIT_CODE_" varchar, "C"."PERIOD_CODE_" varchar, "C"."REMARK_" varchar, '
        #        '"C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar, "C"."STATUS_" varchar,'
        #        '"C"."CODE_" varchar, "C"."NAME_" varchar, "C"."TIME_LIMIT_" varchar,'
        #        '"C"."YIELD_RATE_" varchar, "C"."BREAKEVEN_" varchar, "C"."START_FUNDS_" varchar,'
        #        '"C"."INVEST_PERIOD_" varchar, "C"."SALE_DISTRICT_" varchar, "C"."SALE_START_" varchar,'
        #        '"C"."SALE_END_" varchar, "C"."RISK_LEVEL_" varchar, "C"."REDEMING_MODE_" varchar,'
        #        '"C"."PRIVATE_BANK_" varchar, "C"."URL_" varchar, "C"."DEALTIME_" varchar, "C"."DATETIME_" varchar,'
        #        '"C"."ENTITY_NAME_" varchar, "C"."CURRENCY_TYPE_" varchar, "C"."INCREASE_UNIT_" varchar,'
        #        '"C"."YIELD_START_DATE_" varchar, "C"."YIELD_END_DATE_" varchar, "C"."YIELD_TYPE_" varchar,'
        #        '"C"."TARGET_" varchar, "C"."PRODUCT_TYPE_" varchar, "C"."YIELD_STATMENT_" varchar,'
        #        '"C"."INVEST_RANGE_" varchar, "C"."PRE_STOP_" varchar, "C"."RASE_PLAN_" varchar,'
        #        '"C"."PURCHASE_" varchar, "T"."CONTENT_" varchar, "C"."IMAGE_" varchar) IMMUTABLE_ROWS = true')
        #
        # # 创建表
        # p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 增加列
        # p_client.add_column_phoenix(connection=connection, column="IMAGE_")

        for entity in ["CHINANETFINANCIAL", "JSFIN_CCBDATA"]:
            # for entity in self.entity_list:
            status = False
            module_name = __import__(entity)
            self.logger.info("开始进行 ENTITY_CODE_: {}".format(entity))
            self.remove_id_list = []
            self.copy_mongo_data_list = []
            # find_id = "5c3f118f8d7fee068da6ef53"
            find_id = None
            try:
                if entity == "JSFIN_CCBDATA":
                    m_client.mongo_collection = "JSFIN_CCBDATA"
                    collection = m_client.get_check_collection(db=db, collection_list=collection_list)
                    mongo_data_list = module_name.ScriptCCB.get_data_from_mongo(self=self, m_client=m_client,
                                                                                collection=collection,
                                                                                data_id=None)
                else:
                    m_client.mongo_collection = "FINPRODUCT_FINASSIST"
                    collection = m_client.get_check_collection(db=db, collection_list=collection_list)
                    mongo_data_list = self.get_data_from_mongo(m_client=m_client,
                                                               collection=collection, entity_code=entity,
                                                               data_id=find_id)
            except pymongo.errors.ServerSelectionTimeoutError:
                sleep(1)
                if entity == "JSFIN_CCBDATA":
                    m_client.mongo_collection = "JSFIN_CCBDATA"
                    mongo_data_list = module_name.ScriptCCB.get_data_from_mongo(self=self, m_client=m_client,
                                                                                collection=collection,
                                                                                data_id=None)
                else:
                    m_client.mongo_collection = "FINPRODUCT_FINASSIST"
                    collection = m_client.get_check_collection(db=db, collection_list=collection_list)
                    mongo_data_list = self.get_data_from_mongo(m_client=m_client,
                                                               collection=collection, entity_code=entity,
                                                               data_id=find_id)

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
                        if entity == "CHINANETFINANCIAL":
                            re_data = module_name.data_shuffle(data=data, sales_status=self.sales_status,
                                                               produc_category=self.produc_category,
                                                               revenue_type=self.revenue_type,
                                                               operaton_pattern=self.operaton_pattern,
                                                               purchase_amount=self.purchase_amount,
                                                               duration_type=self.duration_type)
                        elif entity == "JSFIN_CCBDATA":
                            re_data = module_name.ScriptCCB.data_shuffle(self=self, data=data)
                        else:
                            re_data = module_name.data_shuffle(data)

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
                continue
            break
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
    script_bidding = FinProductScript()
    script_bidding.run()
