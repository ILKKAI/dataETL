# -*- coding: utf-8 -*-

# todo  alter table "CommonBidding" add "F"."CREATE_TIME_" VARCHAR

# 获取当前文件路径
import hashlib
import sys
import os
from copy import deepcopy

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])


from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
import time
from log.data_log import Logger
import pymongo


class AllToPhoenix(object):
    def __init__(self):
        self.file_list = list()
        self.get_code_list()
        self.logger = Logger().logger
        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.copy_mongo_data_list = list()
        self.remove_id_list = list()
        # 字段验证列表
        self.verify_list = ["ID_", "CONTENT_", "NOTICE_TIME_", "TITLE_", "PROJECT_NAME_", "BID_CONTENT_",
                            "SIGN_START_TIME_", "SIGN_END_TIME_", "OPEN_BID_TIME_", "OPEN_BID_PLACE_",
                            "BID_AGENCY_", "APPLY_CONDITION_", "SIGN_QUALIFICATION_", "PROJECT_ID_",
                            "WIN_CANDIDATE_", "CANDIDATE_RANK_", "BID_", "URL_", "DEALTIME_", "CREATE_TIME_",
                            "ENTITY_NAME_", "ENTITY_CODE_", "ENTITY_STATUS_", "SIGN_MATERIAL_", "BID_TYPE_",
                            "DATETIME_", "BUDGET_PRICE_", "PASS_REASON_", "PRESALE_CONTENT_", "PRESALE_WAY_",
                            "PRESALE_START_TIME_", "PRESALE_END_TIME_", "PRESALE_ADDR_", "PRESALE_PREPARE_", "IMAGE_"]

    def get_code_list(self):
        """
        获取当前目录下文件名(去除 "CommonBidding_" 后就是 ENTITY_CODE_ )
        :return:
        """
        for root, dirs, files in os.walk(curPath):
            # print(root)  # 当前目录路径
            # print(dirs)  # 当前路径下所有子目录
            # print(files)  # 当前路径下所有非目录子文件
            self.file_list = files
            self.file_list.remove("__init_____.py")
            break

    def get_data_from_mongo(self, m_client, collection, entity_code):
        """

        :param m_client: MongoDB client
        :param collection: MongoDB collection
        :param entity_code:
        :return: all from MongoDB where ENTITY_CODE_ = entity_code
        """
        m_client.mongo_db = "spider_data"
        other_query = {"$or": [{"TITLE_": {"$exists": True}}, {"title": {"$exists": True}}]}
        try:
            mongo_data_list = m_client.get_data_from_mongodb(collection=collection, entity_code=entity_code,
                                                             exclude_code=None, limit_number=None,
                                                             other_query=other_query)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            time.sleep(1)
            mongo_data_list = m_client.get_data_from_mongodb(collection=collection, entity_code=entity_code,
                                                             exclude_code=None, limit_number=None,
                                                             other_query=other_query)
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
        :param remove_id_list: delete data id's list
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

    def shuffle_data(self, data):
        """
        通用清洗
        :param data:
        :return:
        """
        re_data = dict()
        if "TITLE_" in data:
            if not data["TITLE_"]:
                return
            hash_m = hashlib.md5()
            hash_m.update(str(data["TITLE_"]).encode("utf-8"))
            hash_title = hash_m.hexdigest()
            row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)
        elif "title" in data:
            if not data["title"]:
                return
            hash_m = hashlib.md5()
            hash_m.update(data["title"].encode("utf-8"))
            hash_title = hash_m.hexdigest()
            row_key = str(data["entity_code"]) + "_" + str(hash_title)
        else:
            return
        re_data["ID_"] = row_key

        for key, value in data.items():
            # 字段验证
            if key in self.verify_list:
                re_data[key] = value
            elif key == "entityStatus" or key == "ENTITY_STATUS_":
                key = "ENTITY_STATUS_"
                value = "DRAFT"
                re_data[key] = value
        if "ENTITY_STATUS_" not in re_data:
            re_data["ENTITY_STATUS_"] = "DRAFT"

        for s_key in self.verify_list:
            if s_key == "CONTENT_" or s_key == "URL_" or s_key == "url":
                continue
            if data.get(s_key, ""):
                data[s_key] = data[s_key].replace("|", "")

        return re_data

    def run(self):
        # 创建 Phoenix 对象
        p_client = PhoenixHbase(table_name="CommonBidding")
        # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="CommonBidding")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)
        # 创建 MongoDB spider_data_old 数据库对象
        old_client = MongoClient(mongo_collection="CommonBidding")
        # 本地测试
        # old_client.client = pymongo.MongoClient(host="localhost", port=27017,
        #                                         serverSelectionTimeoutMS=60, connectTimeoutMS=60, connect=False)
        old_client.mongo_db = "spider_data_old"
        db_old, collection_list_old = old_client.client_to_mongodb()
        collection_old = db_old["CommonBidding"]

        # 删除表
        # p_client.drop_table_phoenix(connection=connection)

        # 招投标表创建语句
        # sql = ('create table "CommonBidding" ("ID_" varchar primary key, "F"."CONTENT_" varchar,'
        #        '"F"."NOTICE_TIME_" varchar,"F"."TITLE_" varchar,"F"."PROJECT_NAME_" varchar,'
        #        '"F"."BID_CONTENT_" varchar, "F"."SIGN_START_TIME_" varchar, "F"."SIGN_END_TIME_" varchar,'
        #        '"F"."OPEN_BID_TIME_" varchar, "F"."OPEN_BID_PLACE_" varchar, "F"."BID_AGENCY_" varchar,'
        #        '"F"."APPLY_CONDITION_" varchar, "F"."SIGN_QUALIFICATION_" varchar, "F"."PROJECT_ID_" varchar,'
        #        '"F"."WIN_CANDIDATE_" varchar, "F"."CANDIDATE_RANK_" varchar, "F"."BID_" varchar,"F"."URL_" varchar,'
        #        '"F"."DEALTIME_" varchar, "F"."ENTITY_NAME_" varchar, "F"."ENTITY_CODE_" varchar,'
        #        '"F"."ENTITY_STATUS_" varchar, "F"."SIGN_MATERIAL_" varchar, "F"."BID_TYPE_" varchar,'
        #        '"F"."DATETIME_" varchar, "F"."BUDGET_PRICE_" varchar, "F"."PASS_REASON_" varchar,'
        #        '"F"."PRESALE_CONTENT_" varchar, "F"."PRESALE_WAY_" varchar,"F"."PRESALE_START_TIME_" varchar,'
        #        '"F"."PRESALE_END_TIME_" varchar,"F"."PRESALE_ADDR_" varchar,"F"."PRESALE_PREPARE_" varchar,'
        #        '"F"."IMAGE_" varchar) IMMUTABLE_ROWS = true')
        # 创建表
        # p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 增加列
        # p_client.add_column_phoenix(connection=connection, column="IMAGE_")

        # 遍历 ENTITY_CODE_ 列表
        # self.file_list = ["CommonBidding_86JCW"]
        for f in self.file_list:
            status = False
            entity_code = f.replace(".py", "")
            module_name = __import__(entity_code)
            entity_code_mongo = entity_code.replace("CommonBidding_", "")
            self.logger.info("开始进行 ENTITY_CODE_ {}".format(entity_code_mongo))
            self.remove_id_list = []
            self.copy_mongo_data_list = []
            try:
                mongo_data_list = self.get_data_from_mongo(m_client=m_client,
                                                           collection=collection, entity_code=entity_code_mongo)
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(1)
                mongo_data_list = self.get_data_from_mongo(m_client=m_client,
                                                           collection=collection, entity_code=entity_code_mongo)

            # 清洗数据并插入 HBase
            if mongo_data_list:
                once_count = 0
                try:
                    self.find_count += mongo_data_list.count()
                except pymongo.errors.ServerSelectionTimeoutError:
                    time.sleep(1)
                    self.find_count += mongo_data_list.count()
                for data in mongo_data_list:
                    data_id = data["_id"]
                    self.remove_id_list.append(data_id)
                    del data["_id"]
                    # 深拷贝源数据，用于插入 spider_data 库中
                    copy_data = deepcopy(data)
                    self.copy_mongo_data_list.append(copy_data)
                    # 数据清洗
                    try:
                        re_data = module_name.data_shuffle(data)
                        final_data = self.shuffle_data(re_data)
                    except Exception as e:
                        self.remove_id_list.remove(data_id)
                        self.copy_mongo_data_list.remove(copy_data)
                        self.logger.warning("清洗错误,错误 _id 为{}, {}".format(data_id, e))
                        continue
                    # phoenix_HBase 插入数据
                    if final_data:
                        try:
                            p_client.upsert_to_phoenix_by_one(connection=connection, data=final_data)
                            once_count += 1
                        except Exception as e:
                            self.remove_id_list.remove(data_id)
                            self.copy_mongo_data_list.remove(copy_data)
                            self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                            continue
                if once_count > 0:
                    status = True
                    self.logger.info("HBase 插入成功, 成功条数 {}".format(once_count))

                # 删除数据
                if status:
                    delete_count = self.delete_data_from_mongo(m_client=m_client, collection=collection,
                                                               entity_code=entity_code_mongo,
                                                               remove_id_list=self.remove_id_list)
                    self.remove_count += delete_count
                else:
                    self.logger.info("HBase 插入成功条数0条, 不执行删除")

                # 将数据插入 spider_data_old 中
                if status:
                    try:
                        old_client.mongo_db = "spider_data_old"
                        insert_count = old_client.all_to_mongodb(collection=collection_old,
                                                                 insert_list=self.copy_mongo_data_list)
                        self.old_count += insert_count
                        # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
                    except pymongo.errors.ServerSelectionTimeoutError as e:
                        time.sleep(1)
                        self.logger.info("MongoDB 连接失败, 正在重新连接 {}".format(e))
                        insert_count = old_client.all_to_mongodb(collection=collection_old,
                                                                 insert_list=self.copy_mongo_data_list)
                        self.old_count += insert_count
                        # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
                    except Exception as e:
                        self.logger.info(e)

        # 关闭连接
        m_client.client_close()
        p_client.close_client_phoenix(connection=connection)
        self.logger.info("本次共向 MongoDB 查取数据{}条".format(self.find_count))
        self.logger.info("本次共向 HBase 插入数据{}条".format(p_client.count))
        self.logger.info("本次共向 MongoDB 删除数据{}条".format(self.remove_count))
        self.logger.info("本次共向 MongoDB 插入数据{}条".format(self.old_count))
        self.logger.handlers.clear()


if __name__ == '__main__':
    script_bidding = AllToPhoenix()
    script_bidding.run()
