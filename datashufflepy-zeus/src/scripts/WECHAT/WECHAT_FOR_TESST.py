# -*- coding: utf-8 -*-

# 微信 WECHAT
import hashlib
import json
import random
import re
import sys
import os
import time
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import jaydebeapi
import pymongo
import requests
from lxml.etree import HTML


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])

from log.data_log import Logger
from database._phoenix_hbase import PhoenixHbase


class WechatScript(object):
    def __init__(self, entity_type="FOR_TEST_WECHAT"):
        """
        初始化参数
        :param entity_type: WECHAT
        """
        self.entity_type = entity_type
        self.logger = Logger().logger

        # 创建 Phoenix 对象
        self.p_client = PhoenixHbase(table_name=self.entity_type)
        # 连接 Phoenix
        self.connection = self.p_client.connect_to_phoenix()

        self.remove_id_list = list()
        self.copy_mongo_data_list = list()

        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.error_count = 0
        self.data_id = ""
        self.row_key_count = 0

    # def write_to_local(self, data):
    #     with open("wechat_test.txt", "a", encoding="utf-8") as f:
    #         f.write(str(data))

    def read_from_local(self):
        with open("wechat_test.txt", "r", encoding="utf-8") as f:
            data = f.read()
            return data

    def main(self, data):
        """

        :return:
        """
        # # 删除表
        # self.p_client.drop_table_phoenix(connection=self.connection)
        # # quit()

        # # 建表语句
        # table_sql = ('create table "FOR_TEST_WECHAT" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #              '"C"."URL_" varchar, "C"."AREA_CODE_" varchar, "C"."BANK_CODE_" varchar,'
        #              '"C"."BANK_NAME_" varchar, "C"."UNIT_CODE_" varchar, "C"."PERIOD_CODE_" varchar,'
        #              '"C"."REMARK_" varchar, "C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar, '
        #              '"T"."CONTENT_" varchar, "C"."CONTENT_TYPE_" varchar, "C"."TITLE_" varchar,'
        #              '"C"."WECHAT_ID_" varchar, "C"."WECHAT_NAME_" varchar, "C"."ENTITY_NAME_" varchar,'
        #              '"C"."DEALTIME_" varchar,"C"."STATUS_" varchar) IMMUTABLE_ROWS = true')
        #
        # # 创建表
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        i = random.randint(0, 100000000)
        re_data = eval(data)
        re_data["ID_"] = re_data["ID_"] + "_" + str(i)

        if self.success_count % 10 == 0:
            self.logger.info("HBase 插入成功 {} 条".format(self.success_count))
        # 向 HBase 插入数据
        try:
            count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=re_data)
            self.success_count += count
            return self.success_count
        except jaydebeapi.DatabaseError as e:
            self.logger.info("错误 id: {}, 错误信息 {}".format(self.data_id, e))
            return None

    def final(self):
        self.logger.info("本次共向 MongoDB 查取数据{}条".format(self.find_count))
        self.logger.info("本次共向 HBase 插入数据{}条".format(self.success_count))
        self.logger.info("本次共向 MongoDB 删除数据{}条".format(self.remove_count))
        self.logger.info("本次共向 MongoDB 插入数据{}条".format(self.old_count))
        self.logger.info("本次坏数据共 {} 条".format(self.bad_count))
        self.logger.handlers.clear()


if __name__ == '__main__':
    script = WechatScript()
    data = script.read_from_local()
    for i in range(1000000):
        script.main(data=data)
    script.final()

    # with ThreadPoolExecutor(5) as executor:
    #     all_task = list()
    #
    #     while True:
    #         all_task.append(executor.submit(script.main))
    #         for future in as_completed(all_task):
    #             data = future.result()
    #             if data is not None:
    #                 print(data)
