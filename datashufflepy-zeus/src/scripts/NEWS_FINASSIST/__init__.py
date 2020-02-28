# -*- coding: utf-8 -*-

import hashlib
import sys
import os
import pandas as pd
from copy import deepcopy

import re

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath[:-8])
sys.path.append(rootPath[:-8])

from scripts.NEWS_FINASSIST import (CAIJINGNEWS, CNINFONEWS, CSFINACIAL, CSFINACIALNEWS, CSNEWS, CSNOTICE,
                                    FINAQQNEWS, HOUSEQQNEWS, NEWS10JQKA, NEWS10JQKA2, NEWS163DOM, WYCJGSNEWS,
                                    WYCJNEWS, XLCJGSNEWS, XLCJNEWS, XLCJYHMKNEWS)

from __config import AI_PATH
from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from time import sleep, time
from log.data_log import Logger
import pymongo


class AllToPhoenix(object):
    def __init__(self):
        # "CNINFONEWS" pdf too long
        self.code_list = ["CAIJINGNEWS", "CNINFONEWS", "CSFINACIAL", "CSFINACIALNEWS", "CSNEWS", "CSNOTICE",
                          "FINAQQNEWS", "XLCJYHMKNEWS", "XLCJNEWS", "XLCJGSNEWS", "WYCJNEWS", "WYCJGSNEWS",
                          "NEWS163DOM", "NEWS10JQKA2", "NEWS10JQKA", "HOUSEQQNEWS"]

        self.logger = Logger().logger
        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        # 插入 spider_data_old 的数据列表
        # self.copy_mongo_data_list = list()
        # 删除 spider_data 的数据 _id 列表
        # self.remove_id_list = list()
        # self.branch_code_list = list()

        self.verify_list = ["ENTITY_CODE_", "ENTITY_NAME_", "URL_", "PERIOD_CODE_", "STATUS_", "REMARK_",
                            "CREATE_TIME_", "UPDATE_TIME_", "BANK_NAME_", "BANK_CODE_",
                            "CONTENT_", "DATA_SOURCE_", "KEYWORDS_", "ENTITY_NAME_", "ID_"]

    # 从 MongoDB 获取数据
    def get_data_from_mongo(self, m_client, collection, entity_code, find_id):
        m_client.mongo_db = "spider_data"
        m_client.mongo_entity_code = entity_code
        try:
            mongo_data_list = m_client.search_from_mongodb(collection=collection, data_id=find_id)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            sleep(1)
            mongo_data_list = m_client.search_from_mongodb(collection=collection, data_id=find_id)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None

    # 从 MongoDB 删除数据
    def delete_data_from_mongo(self, m_client, collection, entity_code, remove_id_list):
        m_client.mongo_entity_code = entity_code

        try:
            remove_count = m_client.remove_from_mongo(collection=collection, remove_id_list=remove_id_list)
            return remove_count
        except pymongo.errors.ServerSelectionTimeoutError:
            remove_count = m_client.remove_from_mongo(collection=collection, remove_id_list=remove_id_list)
            return remove_count
        except Exception as e:
            self.logger.info(e)
            return None

    def get_brief_from_ai(self, data):
        data["CONTENT_"] = data["CONTENT_"].replace("|", "")
        if data["PUBLISH_TIME_"]:
            data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"][:10]
        # ID
        hash_m = hashlib.md5()
        hash_m.update(data["URL_"].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        data["ID_"] = data["ENTITY_CODE_"] + "_" + str(hash_title)

        text = data["CONTENT_"]

        ex_line = ("python3" + " " + AI_PATH + " " + '\"{}\"'.format(text) + " " + "1")
        # print(ex_line)
        r = os.popen(ex_line)

        # print(1, r.read())
        data["BRIEF_"] = r.read()

        return data

    # 主函数
    def run(self):
        # # 创建 Phoenix 对象
        p_client = PhoenixHbase(table_name="NEWS_FINASSIST")
        p_client.verify_list = self.verify_list
        # # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="NEWS_FINASSIST")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)

        # # 删除表
        # p_client.drop_table_phoenix(connection=connection)
        #
        # # 表创建语句
        # sql = ('create table "NEWS_FINASSIST" ("ID_" varchar primary key, "T"."CONTENT_" varchar, '
        #        '"C"."ENTITY_NAME_" varchar, "C"."ENTITY_CODE_" varchar, "C"."TITLE_" varchar, "C"."BRIEF_" varchar, '
        #        '"C"."PUBLISH_TIME_" varchar, "C"."KEYWORDS_" varchar, "C"."URL_" varchar, "C"."DATA_SOURCE_" varchar,'
        #        '"C"."AREA_CODE_" varchar, "C"."BANK_CODE_" varchar, "C"."BANK_NAME_" varchar,'
        #        '"C"."UNIT_CODE_" varchar, "C"."PERIOD_CODE_" varchar, "C"."REMARK_" varchar,'
        #        '"C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar, "F"."STATUS_" varchar)'
        #        'IMMUTABLE_ROWS = true')

        # # 创建表
        # p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 遍历 ENTITY_CODE_ 列表
        for entity_code in self.code_list:
            status = False
            module_name = __import__(entity_code)
            self.logger.info("开始进行 ENTITY_CODE_ {}".format(entity_code))

            # self.remove_id_list = []
            # self.copy_mongo_data_list = []
            # self.branch_code_list = []
            if entity_code == "CAIJINGNEWS":
                find_id = "5c6bfa508d7fee512a4ca68f"
            else:
                find_id = ""
            # find_id = ""
            try:
                mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection,
                                                           entity_code=entity_code, find_id=find_id)
            except pymongo.errors.ServerSelectionTimeoutError:
                sleep(1)
                mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection,
                                                           entity_code=entity_code, find_id=find_id)

            # 清洗数据并插入 HBase
            if mongo_data_list:
                once_count = 0
                self.find_count = mongo_data_list.count()
                for i in range(1000000):
                    try:
                        data = mongo_data_list.__next__()
                    except pymongo.errors.ServerSelectionTimeoutError:
                        continue
                    except StopIteration:
                        break

                    # for data in mongo_data_list:
                    data_id = data["_id"]
                    if self.success_count % 100 == 0:
                        self.logger.info("running on data_id: {}".format(data_id))
                    # print(data_id)
                    # copy_data = {}
                    # self.remove_id_list.append(data_id)
                    try:
                        del data["_id"]
                        # copy_data = deepcopy(data)
                        # self.copy_mongo_data_list.append(copy_data)
                        data_list = [data]
                        re_data = module_name.data_shuffle(data_list)

                        if not re_data:
                            self.bad_count += 1
                            # self.remove_id_list.remove(data_id)
                            continue
                    except Exception as e:
                        # self.remove_id_list.remove(data_id)
                        # self.copy_mongo_data_list.remove(copy_data)
                        self.logger.warning("清洗错误,错误 _id 为{}, {}".format(data_id, e))
                        continue

                    if isinstance(re_data, list):
                        for list_data in re_data:
                            # phoenix_HBase 插入数据
                            if list_data:
                                try:
                                    if entity_code != "CNINFONEWS":
                                        ai_data = self.get_brief_from_ai(data=list_data)
                                    else:
                                        ai_data = list_data
                                    # print(ai_data["CONTENT_"])
                                except Exception as e:
                                    self.logger.info("AI 调取失败, 错误信息", e)
                                    ai_data = re_data
                                try:
                                    success_count = p_client.upsert_to_phoenix_by_one(connection=connection,
                                                                                      data=ai_data)
                                    once_count += success_count
                                    self.success_count += success_count
                                    if self.success_count % 10 == 0:
                                        self.logger.info("HBase 插入成功, 成功条数{}条".format(once_count))
                                except Exception as e:
                                    # self.remove_id_list.remove(data_id)
                                    # self.copy_mongo_data_list.remove(copy_data)
                                    self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                                    continue
                                try:
                                    # 添加 {d:1}
                                    update_count = m_client.update_to_mongodb(collection=collection, data_id=data_id,
                                                                              data_dict={"d": 1})
                                    self.remove_count += update_count
                                    # self.logger.info("MongoDB 更新成功")
                                    if self.remove_count % 10 == 0:
                                        self.logger.info("MongoDB 更新成功, 成功条数 {} 条".format("10"))
                                except Exception as e:
                                    # self.remove_id_list.remove(data_id)
                                    # self.copy_mongo_data_list.remove(copy_data)
                                    self.logger.warning("MongoDB 更新 _id 为 {} 的数据失败, {}".format(data_id, e))
                                    continue

                    elif isinstance(re_data, dict):
                        # phoenix_HBase 插入数据
                        if re_data:
                            try:
                                success_count = p_client.upsert_to_phoenix_by_one(connection=connection, data=re_data)
                                once_count += success_count
                                self.success_count += success_count
                                self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                            except Exception as e:
                                # self.remove_id_list.remove(data_id)
                                # self.copy_mongo_data_list.remove(copy_data)
                                self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                                continue

                if once_count > 0:
                    status = True
                    self.logger.info("ENTITY_CODE_: {} 插入成功条数 {}".format(entity_code, once_count))
                mongo_data_list.close()
            else:
                continue

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
    script_bidding = AllToPhoenix()
    script_bidding.run()
