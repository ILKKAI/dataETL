# -*- coding: utf-8 -*-
"""金融界信托网"""
import hashlib

import arrow
import re

import jaydebeapi
import pymongo
import time

from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger


class Entrust(object):
    def __init__(self):
        # 创建 MongoDB 对象
        self.m_client = MongoClient(mongo_collection="JSENTRUST_CCBDATA")
        db, collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=db, collection_list=collection_list)

        # 创建 Phoenix 对象
        self.p_client = PhoenixHbase(table_name="ENTRUST")
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
        hash_m.update(data["NAME_"].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)
        re_data["ID_"] = row_key
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        re_data["STATUS_"] = "1"
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["URL_"] = data["URL_"]

        if data["ENTITY_CODE_"] == "CHINATRC":
            # "C"
            # re_data["AREA_CODE_"]
            # re_data["BANK_CODE_"]
            # re_data["BANK_NAME_"]
            # re_data["UNIT_CODE_"]

            pub_date = eval(data["PUB_DATE_"])
            date = str(pub_date["time"])[:-3]
            t = arrow.get(int(date))
            publish_date = str(t)[:10]
            period_code = publish_date.replace("-", "")
            re_data["PERIOD_CODE_"] = period_code
            # re_data["REMARK_"]

            # re_data["UPDATE_TIME_"]

            re_data["CODE_"] = data["CODE_"]
            re_data["NAME_"] = data["NAME_"]
            re_data["ISSUER_"] = data["ISSUER_"]
            re_data["FUNCTION_"] = data["FUNCTION_"]

            pro_date = eval(data["PRO_START_"])
            pro_date = str(pro_date["time"])[:-3]
            p_t = arrow.get(int(pro_date))
            product_date = str(p_t)[:10]
            re_data["PRO_START_"] = product_date
            re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
            re_data["RUN_MODE_"] = data["RUN_MODE_"]
            re_data["INDUSTRY_"] = data["INDUSTRY_"]
            re_data["PUB_DATE_"] = publish_date
            # re_data["SCALE_"] = data[""]
            # re_data["MONTH_"]
            # re_data["YIELD_RATE_"]
            # re_data["START_FUNDS_"]
            # re_data["PURPOSE_"]
            # re_data["ESTAB_ANNOUNCEMENT_"]
            # re_data["ENTRUST_STATUS_"]
            #
            # re_data["DISTRIBU_MODE_"]
            # re_data["INVEST_AREA_"]
            # re_data["TERM_TYPE_"] = data["TERM_TYPE_"]
            # re_data["INVEST_DIRECTION_"]
            # re_data["INVEST_MODE_"] = data["INVEST_MODE_"]
            # re_data["CURRENCY_"]
            # re_data["MANAGE_TYPE_"]
            # re_data["SALE_TARGET_"]
            # re_data["PROFIT_TYPE_"] = data["PROFIT_TYPE_"]
            # re_data["ISSUER_AREA_"]
            # re_data["RESERVE_INFO_"]
            # re_data["TRUSTEESHIP_BANK_"]
            # re_data["OTHER_INFO_"]
            # re_data["OTHER_INFO_"]
        elif data["ENTITY_CODE_"] == "TRUSTHEXUN":
            # "C"
            # re_data["AREA_CODE_"]
            # re_data["BANK_CODE_"]
            # re_data["BANK_NAME_"]
            # re_data["UNIT_CODE_"]
            re_data["PERIOD_CODE_"] = data["PUB_DATE_"].replace("-", "")
            # re_data["REMARK_"]
            # re_data["UPDATE_TIME_"]
            # re_data["CODE_"] = data["CODE_"]
            re_data["NAME_"] = data["NAME_"]
            re_data["ISSUER_"] = data["ISSUER_"]
            # re_data["FUNCTION_"] = data["FUNCTION_"]
            # re_data["PRO_START_"]
            re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"].replace("至月", "")
            # re_data["RUN_MODE_"] = data["RUN_MODE_"]
            re_data["INDUSTRY_"] = data["INDUSTRY_"]
            re_data["PUB_DATE_"] = data["PUB_DATE_"]
            re_data["SCALE_"] = data["SCALE_"]
            # re_data["MONTH_"]
            re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
            re_data["START_FUNDS_"] = data["START_FUNDS_"]
            # re_data["PURPOSE_"]
            # re_data["ESTAB_ANNOUNCEMENT_"]
            # re_data["ENTRUST_STATUS_"]
            #
            # re_data["DISTRIBU_MODE_"]
            # re_data["INVEST_AREA_"]
            # re_data["TERM_TYPE_"] = data["TERM_TYPE_"]
            # re_data["INVEST_DIRECTION_"]
            re_data["INVEST_MODE_"] = data["INVEST_MODE_"]
            re_data["CURRENCY_"] = data["CURRENCY_"]
            re_data["MANAGE_TYPE_"] = data["MANAGE_TYPE_"]
            re_data["SALE_TARGET_"] = data["SALE_TARGET_"]
            re_data["PROFIT_TYPE_"] = data["PROFIT_TYPE_"]
            re_data["ISSUER_AREA_"] = data["ISSUER_AREA_"]
            re_data["RESERVE_INFO_"] = data["RESERVE_INFO_"]
            # re_data["TRUSTEESHIP_BANK_"]
            re_data["OTHER_INFO_"] = data["OTHER_INFO_"]
        elif data["ENTITY_CODE_"] == "YANGLEE":
            # "C"
            # re_data["AREA_CODE_"]
            # re_data["BANK_CODE_"]
            # re_data["BANK_NAME_"]
            # re_data["UNIT_CODE_"]
            re_data["PERIOD_CODE_"] = data["PUB_DATE_"].replace("-", "")
            # # re_data["REMARK_"]
            # # re_data["UPDATE_TIME_"]
            # re_data["CODE_"] = data["CODE_"]
            re_data["NAME_"] = data["NAME_"]
            re_data["ISSUER_"] = data["ISSUER_"]
            # re_data["FUNCTION_"] = data["FUNCTION_"]
            # # re_data["PRO_START_"]
            re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
            # re_data["RUN_MODE_"] = data["RUN_MODE_"]
            re_data["INDUSTRY_"] = data["INDUSTRY_"]
            re_data["PUB_DATE_"] = data["PUB_DATE_"]
            # re_data["SCALE_"] = data["SCALE_"]
            # # re_data["MONTH_"]
            re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
            re_data["START_FUNDS_"] = data["START_FUNDS_"]
            # # re_data["PURPOSE_"]
            # # re_data["ESTAB_ANNOUNCEMENT_"]
            re_data["ENTRUST_STATUS_"] = data["STATUS_"]
            #
            re_data["DISTRIBU_MODE_"] = data["DISTRIBU_MODE_"]
            # # re_data["INVEST_AREA_"]
            re_data["TERM_TYPE_"] = data["TERM_TYPE_"]
            # # re_data["INVEST_DIRECTION_"]
            # re_data["INVEST_MODE_"] = data["INVEST_MODE_"]
            # re_data["CURRENCY_"] = data["CURRENCY_"]
            # re_data["MANAGE_TYPE_"] = data["MANAGE_TYPE_"]
            # re_data["SALE_TARGET_"] = data["SALE_TARGET_"]
            # re_data["PROFIT_TYPE_"] = data["PROFIT_TYPE_"]
            re_data["ISSUER_AREA_"] = data["ISSUER_AREA_"]
            # re_data["RESERVE_INFO_"] = data["RESERVE_INFO_"]
            re_data["TRUSTEESHIP_BANK_"] = data["TRUSTEESHIP_BANK_"]
            re_data["OTHER_INFO_"] = data["OTHER_INFO_"]
        elif data["ENTITY_CODE_"] == "TRUSTONE":
            # "C"
            # re_data["AREA_CODE_"]
            # re_data["BANK_CODE_"]
            # re_data["BANK_NAME_"]
            # re_data["UNIT_CODE_"]
            re_data["PERIOD_CODE_"] = data["PUB_DATE_"].replace("-", "")
            # # re_data["REMARK_"]
            # # re_data["UPDATE_TIME_"]
            # re_data["CODE_"] = data["CODE_"]
            re_data["NAME_"] = data["NAME_"]
            re_data["ISSUER_"] = data["ISSUER_"]
            # re_data["FUNCTION_"] = data["FUNCTION_"]
            # # re_data["PRO_START_"]
            # re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
            # re_data["RUN_MODE_"] = data["RUN_MODE_"]
            # re_data["INDUSTRY_"] = data["INDUSTRY_"]
            re_data["PUB_DATE_"] = data["PUB_DATE_"]
            re_data["SCALE_"] = data["SCALE_"]
            # # re_data["MONTH_"]
            re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
            re_data["START_FUNDS_"] = data["START_FUNDS_"]
            # # re_data["PURPOSE_"]
            # # re_data["ESTAB_ANNOUNCEMENT_"]
            # re_data["ENTRUST_STATUS_"] = data["STATUS_"]
            # #
            re_data["DISTRIBU_MODE_"] = data["DISTRIBU_MODE_"]
            re_data["INVEST_AREA_"] = data["INVEST_AREA_"]
            re_data["TERM_TYPE_"] = data["TERM_TYPE_"]
            re_data["INVEST_DIRECTION_"] = data["INVEST_DIRECTION_"]
            re_data["INVEST_MODE_"] = data["INVEST_MODE_"]
            # re_data["CURRENCY_"] = data["CURRENCY_"]
            # re_data["MANAGE_TYPE_"] = data["MANAGE_TYPE_"]
            # re_data["SALE_TARGET_"] = data["SALE_TARGET_"]
            re_data["PROFIT_TYPE_"] = data["PROFIT_TYPE_"]
            # re_data["ISSUER_AREA_"] = data["ISSUER_AREA_"]
            re_data["RESERVE_INFO_"] = re.sub(r"</?\w*>", "", data["RESERVE_INFO_"])
            # re_data["TRUSTEESHIP_BANK_"] = data["TRUSTEESHIP_BANK_"]
            # re_data["OTHER_INFO_"] = data["OTHER_INFO_"]

        return re_data

    def run(self):
        # # delete table
        # self.p_client.drop_table_phoenix(connection=self.connection)
        # # quit()
        #
        # # create table sql
        # table_sql = ('create table "ENTRUST" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #              '"C"."ENTITY_NAME_" varchar, "C"."CREATE_TIME_" varchar, "C"."STATUS_" varchar,'
        #              '"C"."DEALTIME_" varchar, "C"."URL_" varchar, "C"."AREA_CODE_" varchar, "C"."FUNCTION_" varchar,'
        #              '"C"."BANK_CODE_" varchar, "C"."BANK_NAME_" varchar, "C"."UNIT_CODE_" varchar,'
        #              '"C"."PERIOD_CODE_" varchar, "C"."REMARK_" varchar, "C"."UPDATE_TIME_" varchar,'
        #              '"C"."CODE_" varchar, "C"."NAME_" varchar, "C"."ISSUER_" varchar, "C"."PRO_START_" varchar,'
        #              '"C"."INVEST_PERIOD_" varchar,"C"."RUN_MODE_" varchar, "C"."INDUSTRY_" varchar,'
        #              '"C"."PUB_DATE_" varchar, "C"."SCALE_" varchar, "C"."MONTH_" varchar, "C"."YIELD_RATE_" varchar,'
        #              '"C"."START_FUNDS_" varchar, "C"."PURPOSE_" varchar, "C"."ESTAB_ANNOUNCEMENT_" varchar,'
        #              '"C"."ENTRUST_STATUS_" varchar, "C"."DISTRIBU_MODE_" varchar, "C"."INVEST_AREA_" varchar,'
        #              '"C"."TERM_TYPE_" varchar, "C"."INVEST_DIRECTION_" varchar, "C"."INVEST_MODE_" varchar,'
        #              '"C"."CURRENCY_" varchar, "C"."MANAGE_TYPE_" varchar, "C"."SALE_TARGET_" varchar,'
        #              '"C"."PROFIT_TYPE_" varchar, "C"."ISSUER_AREA_" varchar, "C"."RESERVE_INFO_" varchar,'
        #              '"C"."TRUSTEESHIP_BANK_" varchar, "C"."OTHER_INFO_" varchar) IMMUTABLE_ROWS = true')
        #
        # # create table
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection, data_id="5c67307d9bb3df76b4229f79")

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
            # try:
            re_data = self.data_shuffle(data=data)
            # except Exception as e:
            #     self.logger.info("数据清洗失败 {}, id: {}".format(e, self.data_id))
            #     continue

            if re_data:
                # upsert data to HBase
                try:
                    success_count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=re_data)
                except jaydebeapi.DatabaseError as e:
                    self.logger.info("错误 id: {}, 错误信息 {}".format(self.data_id, e))
                    continue
                # # add {d:1}
                # try:
                #     self.m_client.update_to_mongodb(collection=self.collection, data_id=self.data_id,
                #                                     data_dict={"d": 1})
                #     self.remove_count += 1
                #     if self.remove_count % 10 == 0:
                #         self.logger.info("MongoDB 更新成功, 成功条数 {}".format(self.remove_count))
                # except Exception as e:
                #     self.logger.info("MongoDB 更新 _id 为 {} 的数据失败, {}".format(self.data_id, e))
                #     continue

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
    script = Entrust()
    script.run()
