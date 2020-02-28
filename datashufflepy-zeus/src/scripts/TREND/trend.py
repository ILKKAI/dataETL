# -*- coding: utf-8 -*-
"""营销活动"""

import hashlib
import random

import jaydebeapi
import pymongo
import time

import re

from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger


class Trend(object):
    def __init__(self):
        # 创建 MongoDB 对象
        self.m_client = MongoClient(mongo_collection="TREND")
        db, collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=db, collection_list=collection_list)

        # 创建 Phoenix 对象
        self.p_client = PhoenixHbase(table_name="CHA_BRANCH_MARKET_ACT")
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
        # HBase row_key
        hash_m = hashlib.md5()
        hash_m.update(data["TITLE_"].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

        # 分行
        copy_result = dict()
        copy_result["ID_"] = row_key
        copy_result["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        copy_result["URL_"] = data["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = data["ENTITY_CODE_"].replace("PRIVATEINFO", "")
        copy_result["BANK_NAME_"] = data["ENTITY_NAME_"].replace("私行动态", "")
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = data["NOTICE_TIME_"].replace("-", "")
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = data["DATETIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "0"
        copy_result["DELETE_STATUS_"] = "0"
        copy_result["DATA_STATUS_"] = "uncheck"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = data["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = result[""]
        # copy_result["HOT_"] = result[""]
        # copy_result["IMPORTANCE_"] = result[""]
        copy_result["ACT_NAME_"] = data["TITLE_"]
        # copy_result["IMAGES_"] = data[""]
        # copy_result["TARGET_"] = data[""]
        # copy_result["BRIEF_"] = data[""]
        copy_result["DETAILS_"] = data["CONTENT_"]
        # copy_result["RULE_"] = data[""]
        # copy_result["START_TIME_"] = data[""]
        # copy_result["END_TIME_"] = data[""]
        # copy_result["ACT_TYPE1_"] = data[""]
        # copy_result["ACT_TYPE2_"] = data[""]
        # copy_result["ACT_TYPE3_"] = data[""]
        copy_result["PUBLISH_TIME_"] = data["NOTICE_TIME_"]
        # copy_result["READS_"] = data[""]
        # copy_result["LIKES_"] = data[""]
        # copy_result["COMMENTS_"] = data[""]
        # copy_result["JOINS_"] = data[""]
        # copy_result["RELAYS_"] = data[""]
        # copy_result["SOURCE_ID_"] = data[""]
        # copy_result["HTML_"] = data[""]
        # copy_result["SOURCE_OWN_NAME_"] = data[""]
        # copy_result["SOURCE_OWN_ID_"] = data[""]
        return copy_result

        # "C"
        # re_data["ID_"] = row_key
        # re_data["TYPE_"] = random.choice(
        #     ["税务法律", "子女教育", "健康医养", "财富管理", "生活娱乐", "旅游出行", "艺术/艺术品", "节日庆贺", "其他"])
        # re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        # re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        # re_data["BANK_CODE_"] = data["ENTITY_CODE_"].replace("PRIVATEINFO", "")
        # re_data["BANK_NAME_"] = data["ENTITY_NAME_"].replace("私行动态", "")
        # # re_data["AREA_CODE_"]
        # # re_data["UNIT_CODE_"]
        # period_code = data["NOTICE_TIME_"].replace("-", "")
        # re_data["PERIOD_CODE_"] = period_code
        # re_data["CONTENT_"] = data["CONTENT_"]
        # re_data["NOTICE_TIME_"] = data["NOTICE_TIME_"]
        # re_data["STATUS_"] = "1"
        # # re_data["REMARK_"] = ""
        # re_data["CREATE_TIME_"] = data["DATETIME_"]
        # # re_data["UPDATE_TIME_"]
        # re_data["TITLE_"] = data["TITLE_"]
        # re_data["URL_"] = data["URL_"]
        # re_data["DEALTIME_"] = data["DEALTIME_"]
        # # re_data["DATETIME_"] = data["DATETIME_"]
        # # re_data["SOURCE_TYPE_"]
        #
        # return re_data

    def run(self):
        # delete table
        # self.p_client.drop_table_phoenix(connection=self.connection)
        # quit()

        # add colum
        # self.p_client.add_column_phoenix(connection=self.connection, column="SOURCE_TYPE_")
        # quit()

        # create table sql
        # table_sql = ('create table "MARKETING_ACT" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #              '"C"."ENTITY_NAME_" varchar, "C"."TITLE_" varchar,"C"."NOTICE_TIME_" varchar,'
        #              '"T"."CONTENT_" varchar,"C"."OBJ_" varchar, "C"."ATENDANCE_" varchar, "C"."PERIOD_CODE_" varchar,'
        #              '"C"."IMAGES_" varchar, "C"."RESULTS_" varchar,"C"."PLACE_" varchar, "C"."TYPE_" varchar,'
        #              '"C"."READ_NUM_" varchar, "C"."CONTENT_NUM_" varchar, "C"."COMMENT_CONTENT_" varchar, '
        #              '"C"."FORWARD_NUM_" varchar, "C"."COLLECTION_NUM_" varchar, "C"."PRAISE_NUM_" varchar,'
        #              '"C"."BANK_NAME_" varchar, "C"."STATUS_" varchar, "C"."REMARK_" varchar, "C"."SOURCE_ID_" varchar,'
        #              '"C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar,"C"."SOURCE_" varchar,'
        #              '"C"."URL_" varchar, "C"."BANK_CODE_" varchar, "C"."DEALTIME_" varchar, '
        #              '"C"."SOURCE_TYPE_" varchar, "C"."IMPROTANCE_" varchar) IMMUTABLE_ROWS = true')

        # create table
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection)

        # for i in range(mongo_data_list.count() + 100):
        for i in range(100):
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
    script = Trend()
    script.run()
