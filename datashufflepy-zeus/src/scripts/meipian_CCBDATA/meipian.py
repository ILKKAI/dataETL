# -*- coding: utf-8 -*-
"""美篇"""
import hashlib

import arrow
import jaydebeapi
import pymongo
import time

from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger


class Meipian(object):
    def __init__(self):
        # 创建 MongoDB 对象
        self.m_client = MongoClient(mongo_collection="meipian_CCBDATA")
        db, collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=db, collection_list=collection_list)

        # 创建 Phoenix 对象
        self.p_client = PhoenixHbase(table_name="MEIPIAN_CCBDATA")
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

    def data_shuffle(self, data):
        re_data = dict()
        if data["TITLE_"]:
            # HBase row_key
            hash_m = hashlib.md5()
            hash_m.update(data["TITLE_"].encode("utf-8"))
            hash_title = hash_m.hexdigest()
            row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

            # "C" 通用列族字段
            re_data["ID_"] = row_key
            re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
            # re_data["BANK_NAME_"]

            time_arrary = arrow.get(data["CREATE_TIME"])
            period_code = time_arrary.format("YYYYMMDD")
            publish_time = time_arrary.format("YYYY-MM-DD HH:mm:ss")
            re_data["PERIOD_CODE_"] = str(period_code)
            re_data["PUBLISH_TIME_"] = str(publish_time)
            re_data["STATUS_"] = "UNPROCESSED"
            re_data["CONTENT_"] = data["CONTENT_"]
            re_data["REMARK_"] = ""
            # re_data["AREA_CODE_"]
            # re_data["UNIT_CODE_"]
            re_data["CREATE_TIME_"] = data["DATETIME_"]

            re_data["URL_"] = data["URL_"]
            re_data["TITLE_"] = data["TITLE_"]
            re_data["CONTENT_TYPE_"] = data["CONTENT_TYPE_"]
            re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
            re_data["DEALTIME_"] = str(data["DEALTIME_"])
            re_data["VISIT_COUNT_"] = data["VISIT_COUNT"]
            re_data["PRAISE_COUNT_"] = data["PRAISE_COUNT"]
            re_data["COMMENT_COUNT_"] = data["COMMENT_COUNT"]
            re_data["SOURCE_"] = data["SOURCE_"]

            return re_data
        else:
            return None

    def run(self):
        # # delete table
        # self.p_client.drop_table_phoenix(connection=self.connection)
        # # quit()
        #
        # # create table sql
        # table_sql = ('create table "MEIPIAN_CCBDATA" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #              '"C"."URL_" varchar, "C"."PERIOD_CODE_" varchar, "C"."REMARK_" varchar,'
        #              ' "C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar, "T"."CONTENT_" varchar, '
        #              '"C"."TITLE_" varchar, "C"."CONTENT_TYPE_" varchar, "C"."ENTITY_NAME_" varchar,'
        #              '"C"."VISIT_COUNT_" varchar, "C"."PRAISE_COUNT_" varchar, "C"."COMMENT_COUNT_" varchar,'
        #              '"C"."DEALTIME_" varchar, "C"."SOURCE_" varchar, "C"."PUBLISH_TIME_" varchar,'
        #              '"C"."STATUS_" varchar) IMMUTABLE_ROWS = true')

        # # create table
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)
        # f_id = "5c6fa1328d7fee306de9463d"  # quit()
        # f_id = "5c6fe1ba8d7fee1d44775989"  # quit()
        # f_id = "5c6fdb448d7fee394da6a5fb"  # quit() Exception while executing batch.
        # f_id = "5c6fe1ba8d7fee1d44775989"
        f_id = "5c6fe11b9bb3df6b0ec6168b"  # gt 10M
        mongo_data_list = self.m_client.all_from_mongodb(self.collection, data_id=f_id)
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
            print(data["_id"])
            # todo remove and upsert data from mongo

            # shuffle data
            try:
                re_data = self.data_shuffle(data=data)
            except Exception as e:
                self.logger.info("数据清洗失败 {}, id: {}".format(e, self.data_id))
                continue

            if re_data:
                # upsert data to HBase
                try:
                    success_count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=re_data)
                except jaydebeapi.DatabaseError as e:
                    self.logger.info("错误 id: {}, 错误信息 {}".format(self.data_id, e))
                    continue
                # add {d:1}
                try:
                    self.m_client.update_to_mongodb(collection=self.collection, data_id=self.data_id,
                                                    data_dict={"d": 1})
                    self.remove_count += 1
                    if self.remove_count % 10 == 0:
                        self.logger.info("MongoDB 更新成功, 成功条数 {}".format(self.remove_count))
                except Exception as e:
                    self.logger.info("MongoDB 更新 _id 为 {} 的数据失败, {}".format(self.data_id, e))
                    continue

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
    script = Meipian()
    script.run()
