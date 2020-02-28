# -*- coding: utf-8 -*-
"""微博 WEIBOINFO"""
import hashlib
import sys
import os

import time
from copy import deepcopy

import pymongo
import re
from bson import ObjectId

from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])


class WeiboScript(object):
    # 初始化参数
    def __init__(self, entity_type="WEIBOINFO"):
        self.entity_type = entity_type
        self.logger = Logger().logger
        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.copy_mongo_data_list = list()
        self.remove_id_list = list()
        self.branch_code_list = list()
        self.verify_list = ["ID_", "ENTITY_CODE_", "AREA_CODE_", "BANK_CODE_", "BANK_NAME_", "PERIOD_CODE_", "CONTENT_",
                            "REMARK_", "CREATE_TIME_", "UPDATE_TIME_", "CONTENT_CODE_", "RELAYS_", "PRAISES_",
                            "REPLIES_", "CONTENT_IMAGES_", "CONTENT_URL_", "INFO_COMMENTS_", "ENTITY_NAME_",
                            "DEALTIME_", "STATUS_1"]

    def data_shuffle(self, data):
        re_data = dict()

        bank_code = data["BANK_CODE_"][:-9]
        time_array = time.localtime(int(data["DEALTIME_"]))
        create_time = time.strftime("%Y-%m-%d", time_array)
        # "C"
        hash_m = hashlib.md5()
        hash_m.update(data["CONTENT_URL_"].encode("utf-8"))
        hash_id = hash_m.hexdigest()
        re_data["ID_"] = data["BANK_CODE_"] + "_" + hash_id + "_" + create_time
        re_data["ENTITY_CODE_"] = data["BANK_CODE_"]
        # re_data["AREA_CODE_"] = ""
        re_data["BANK_CODE_"] = bank_code
        re_data["BANK_NAME_"] = data["ENTITY_NAME_"].replace("微博", "")

        period_code = re.findall(r"\d{8}", data["PUBLISH_TIME_"])
        if period_code:
            print(period_code)
            re_data["PERIOD_CODE_"] = period_code[0]
            re_data["NOTICE_TIME_"] = period_code[0][:4] + "-" + period_code[0][4:6] + "-" + period_code[0][6:]
        else:
            re_data["PERIOD_CODE_"] = data["PUBLISH_TIME_"].replace("-", "")
            re_data["NOTICE_TIME_"] = data["PUBLISH_TIME_"]

        re_data["CONTENT_"] = data["CONTENT_"]
        # re_data["REMARK_"] = ""
        re_data["CREATE_TIME_"] = create_time
        # re_data["UPDATE_TIME_"] = ""
        re_data["CONTENT_CODE_"] = data["CONTENT_CODE_"]
        re_data["RELAYS_"] = data["RELAYS_"]
        re_data["PRAISES_"] = data["PRAISES_"]
        re_data["REPLIES_"] = data["REPLIES_"]
        re_data["CONTENT_IMAGES_"] = str(data["CONTENT_IMAGES_"])
        re_data["CONTENT_URL_"] = data["CONTENT_URL_"]
        re_data["INFO_COMMENTS_"] = str(data["INFO_COMMENTS_"])
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        re_data["DEALTIME_"] = str(data["DEALTIME_"])
        re_data["STATUS_"] = "1"

        return re_data

    # 从 MongoDB 删除数据
    def delete_data_from_mongo(self, m_client, collection, remove_id_list):
        m_client.mongo_entity_code = None

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
        p_client = PhoenixHbase(table_name="WEIBOINFO")
        p_client.verify_list = self.verify_list
        # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="WEIBOINFO")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)
        # # 创建 MongoDB spider_data_old 数据库对象
        # old_client = MongoClient(mongo_collection="WEIBOINFO")
        # # 本地测试
        # old_client.client = pymongo.MongoClient(host="localhost", port=27017, serverSelectionTimeoutMS=60,
        #                                         connectTimeoutMS=60, connect=False)
        # old_client.mongo_db = "spider_data_old"
        # db_old, collection_list_old = old_client.client_to_mongodb()
        # collection_old = db_old["ORGANIZE_FINASSIST"]

        # # 删除表
        # p_client.drop_table_phoenix(connection=connection)
        # # quit()
        #
        # # 创建表
        # sql = ('create table "WEIBOINFO" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #        '"C"."BANK_CODE_" varchar, "C"."BANK_NAME_" varchar,"C"."NOTICE_TIME_" varchar,'
        #        '"C"."PERIOD_CODE_" varchar, "C"."REMARK_" varchar, "C"."CREATE_TIME_" varchar, '
        #        '"C"."UPDATE_TIME_" varchar,  "T"."CONTENT_" varchar, "C"."CONTENT_CODE_" varchar,'
        #        '"C"."RELAYS_" varchar, "C"."PRAISES_" varchar, "C"."REPLIES_" varchar, '
        #        '"C"."CONTENT_IMAGES_" varchar, "C"."CONTENT_URL_" varchar,"C"."INFO_COMMENTS_" varchar,'
        #        '"C"."ENTITY_NAME_" varchar, "C"."DEALTIME_" varchar,"C"."STATUS_" varchar,'
        #        '"C"."IMPROTANCE_" varchar) IMMUTABLE_ROWS = true')
        # p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 增加列
        # p_client.add_column_phoenix(connection=connection, column="IMAGE_")

        status = False
        self.logger.info("开始进行 WEIBOINFO")

        # 单条查取
        # while True:
        #     try:
        #         data = collection.find_one()
        #     except pymongo.errors.ServerSelectionTimeoutError:
        #         time.sleep(1)
        #         data = collection.find_one()

        # 批量查取
        find_id = None
        # find_id = "5c347f170f535b166dcba6db"
        # find_id = "5c358e340f535b166d569790"
        # find_id = "5c358e3e0f535b166d569c54"
        # find_id = "5c358e540f535b166d56a68c"

        try:
            mongo_data_list = m_client.all_from_mongodb(collection=collection, data_id=find_id)
        except pymongo.errors.ServerSelectionTimeoutError:
            time.sleep(1)
            mongo_data_list = m_client.all_from_mongodb(collection=collection, data_id=find_id)

        # 清洗数据并插入 HBase
        if mongo_data_list:
            self.find_count += mongo_data_list.count()
            for data in mongo_data_list:

                re_data = ""
                data_id = data["_id"]
                copy_data = {}
                self.remove_id_list.append(data_id)
                try:
                    del data["_id"]
                    copy_data = deepcopy(data)
                    self.copy_mongo_data_list.append(copy_data)
                    re_data = self.data_shuffle(data=data)
                    if not re_data:
                        self.bad_count += 1
                        continue
                except Exception as e:
                    self.remove_id_list.remove(data_id)
                    self.copy_mongo_data_list.remove(copy_data)
                    self.logger.warning("清洗错误,错误 _id 为{}, {}".format(data_id, e))
                    continue

                # phoenix_HBase 插入数据
                try:
                    print(data_id)
                    success_count = p_client.upsert_to_phoenix_by_one(connection=connection, data=re_data)
                    self.success_count += success_count
                    # self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                except Exception as e:
                    self.remove_id_list.remove(data_id)
                    self.copy_mongo_data_list.remove(copy_data)
                    self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                    continue
                if self.success_count > 0:
                    status = True
                if self.success_count % 10 == 0:
                    self.logger.info("HBase 插入成功, 成功条数 {}".format(self.success_count))
        else:
            quit()

            # # 删除数据
            # if status:
            #     delete_count = self.delete_data_from_mongo(m_client=m_client, collection=collection,
            #                                                remove_id_list=self.remove_id_list)
            #     self.remove_count += delete_count
            # else:
            #     self.logger.info("HBase 插入成功条数0条, 不执行删除")
            #
            # # 将数据插入 spider_data_old 中
            # if status:
            #     try:
            #         old_client.mongo_db = "spider_data_old"
            #         insert_count = old_client.all_to_mongodb(collection=collection_old,
            #                                                  insert_list=self.copy_mongo_data_list)
            #         self.old_count += insert_count
            #     except pymongo.errors.ServerSelectionTimeoutError as e:
            #         time.sleep(1)
            #         self.logger.info("MongoDB 连接失败, 正在重新连接 {}".format(e))
            #         insert_count = old_client.all_to_mongodb(collection=collection_old,
            #                                                  insert_list=self.copy_mongo_data_list)
            #         self.old_count += insert_count
            #         # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
            #     except Exception as e:
            #         self.logger.info(e)

        # 关闭连接
        m_client.client_close()
        p_client.close_client_phoenix(connection=connection)
        self.logger.info("本次共向 MongoDB 查取数据{}条".format(self.find_count))
        self.logger.info("本次共向 HBase 插入数据{}条".format(self.success_count))
        self.logger.info("本次共向 MongoDB 删除数据{}条".format(self.remove_count))
        self.logger.info("本次共向 MongoDB 插入数据{}条".format(self.old_count))
        self.logger.info("本次坏数据共 {} 条".format(self.bad_count))
        self.logger.handlers.clear()


if __name__ == '__main__':
    script = WeiboScript()
    script.run()
    # client = pymongo.MongoClient(host="172.22.69.35", port=20000)
    # db = client["spider_data"]
    # collection = db["WEIBOINFO"]
    # find_id = "5c358e540f535b166d56a68c"
    # data = collection.find_one({"_id": ObjectId(find_id)})
    # batch_key = list()
    # batch_value = list()
    # for key, value in data.items():
    #     if key == "INFO_COMMENTS_":
    #         print(value)
    #         print(type(value))
    #         value = str(value).replace("[", "(")
    #         value = value.replace("]", ")")
    #         value = value.replace("\'", "\"")
    #         # print(value)
    #         batch_key.append(key)
    #         batch_value.append(value)
    #         a = [batch_value]
    #         print(a)
    #         break
