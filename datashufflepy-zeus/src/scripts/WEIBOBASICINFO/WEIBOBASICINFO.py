# -*- coding: utf-8 -*-
"""微博基本信息 WEIBOBASICINFO"""

import sys
import os
import time
from copy import deepcopy
import pymongo
from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger
from scripts import GenericScript

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])


class WeiboBasicInfoScript(object):
    # 初始化参数
    def __init__(self, entity_type="WEIBOBASICINFO"):
        self.entity_type = entity_type
        self.logger = Logger().logger
        self.verify_list = ["ID_", "BANK_CODE_", "BANK_NAME_", "PERIOD_TIME_", "AREA_CODE_", "CREATE_TIME_",
                            "WEIBO_CODE_", "MAIN_URL_", "NAME_", "FOCUS_", "FANS_", "COMPANY_URL_", "COMPANY_",
                            "DETAILED_URL_", "VIRIFIED_", "BIREF_", "ENTITY_NAME_", "ENTITY_CODE_", "DEALTIME_",
                            "PROVINCE_NAME_", "PROVINCE_CODE_", "STATUS_1"]
        self.remove_id_list = list()
        self.copy_mongo_data_list = list()
        self.branch_code_list = list()
        self.find_count = 0
        self.bad_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0

    def match_weibo_code(self, match):
        mongo_client = MongoClient(mongo_collection="WEIBOBASICINFO")
        db, collection_list = mongo_client.client_to_mongodb()
        collection = mongo_client.get_check_collection(db, collection_list)
        result = mongo_client.match_from_mongo(collection=collection, match=match, output="WEIBO_CODE_")
        return result

    def data_shuffle(self, data, province_list):
        re_data = dict()
        prov_n = ""
        prov_c = ""

        # # BANK_NAME_ 字典
        # name_dict = {"ICBC": "中国工商银行", "ABC": "中国农业银行", "BOC": "中国银行", "CCB": "中国建设银行",
        #              "BOCOM": "交通银行", "PSBC": "中国邮政储蓄银行", "CZB": "浙商银行", "CBHB": "渤海银行",
        #              "ECITIC": "中信银行", "CEB": "中国光大银行", "HXB": "华夏银行", "CMBC": "中国民生银行",
        #              "CMB": "招商银行", "CIB": "兴业银行", "CGB": "广发银行", "PAB": "平安银行",
        #              "SPDB": "浦发银行", "EBCL": "恒丰银行"}
        province_list, city_list, area_list, dir_area_list, bank_list = GenericScript.data_from_mysql()

        bank_code = data["BANK_CODE_"][:-9]

        bank_name = name_dict[bank_code]

        time_array = time.localtime(int(data["DEALTIME_"]))
        period_time = time.strftime("%Y%m%d", time_array)

        for prov in province_list:
            if prov["NAME_"][:2] in data["LOCATION_"]:
                prov_n = prov["NAME_"]
                prov_c = prov["CODE_"]

        # "C"
        # TODO row_key 时间戳还是年-月-日
        re_data["ID_"] = data["BANK_CODE_"] + "_" + period_time
        re_data["BANK_CODE_"] = bank_code
        re_data["BANK_NAME_"] = bank_name
        re_data["PERIOD_CODE_"] = period_time
        re_data["AREA_CODE_"] = prov_c
        re_data["CREATE_TIME_"] = period_time

        re_data["WEIBO_CODE_"] = data["WEIBO_CODE_"]
        re_data["MAIN_URL_"] = data["MAIN_URL_"]
        re_data["NAME_"] = data["NAME_"]
        re_data["FOCUS_"] = data["FOCUS_"]
        re_data["FANS_"] = data["FANS_"]
        re_data["COMPANY_URL_"] = data["COMPANY_URL_"]
        if "COMPANY_" not in data:
            re_data["COMPANY_"] = data["VIRIFIED_"]
        else:
            re_data["COMPANY_"] = data["COMPANY_"]
        re_data["DETAILED_URL_"] = data["DETAILED_URL_"]
        re_data["VIRIFIED_"] = bank_name + "股份有限公司"
        re_data["BIREF_"] = data["BIREF_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        re_data["ENTITY_CODE_"] = data["BANK_CODE_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["PROVINCE_NAME_"] = prov_n
        re_data["PROVINCE_CODE_"] = prov_c

        re_data["STATUS_"] = ""

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
        p_client = PhoenixHbase(table_name="WEIBOBASICINFO")
        p_client.verify_list = self.verify_list
        # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="WEIBOBASICINFO")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)
        # # 创建 MongoDB spider_data_old 数据库对象
        # old_client = MongoClient(mongo_collection="WEIBOBASICINFO")
        # # 本地测试
        # old_client.client = pymongo.MongoClient(host="localhost", port=27017, serverSelectionTimeoutMS=60,
        #                                         connectTimeoutMS=60, connect=False)
        # old_client.mongo_db = "spider_data_old"
        # db_old, collection_list_old = old_client.client_to_mongodb()
        # collection_old = db_old["ORGANIZE_FINASSIST"]

        # 获取地区编码
        province_list, city_list, area_list, dir_area_list = (
            GenericScript(entity_code=None, entity_type=None).area_from_mysql())

        # 删除表
        p_client.drop_table_phoenix(connection=connection)
        # quit()

        # 创建表
        sql = ('create table "WEIBOBASICINFO" ("ID_" varchar primary key, "C"."BANK_CODE_" varchar,'
               '"C"."BANK_NAME_" varchar, "C"."PERIOD_CODE_" varchar, "C"."CREATE_TIME_" varchar,'
               '"C"."UPDATE_TIME_" varchar, "C"."REMARK_" varchar, "C"."WEIBO_CODE_" varchar, "C"."MAIN_URL_" varchar,'
               '"C"."NAME_" varchar, "C"."FOCUS_" varchar, "C"."FANS_" varchar, "C"."COMPANY_URL_" varchar,'
               '"C"."COMPANY_" varchar, "C"."DETAILED_URL_" varchar, "C"."VIRIFIED_" varchar,"C"."AREA_CODE_" varchar,'
               '"C"."BIREF_" varchar, "C"."ENTITY_NAME_" varchar, "C"."ENTITY_CODE_" varchar,'
               '"C"."DEALTIME_" varchar,"C"."PROVINCE_NAME_" varchar, "C"."PROVINCE_CODE_" varchar,'
               '"C"."STATUS_" varchar) IMMUTABLE_ROWS = true')
        p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 增加列
        # p_client.add_column_phoenix(connection=connection, column="IMAGE_")

        # 遍历 ENTITY_CODE_ 列表
        status = False
        self.logger.info("开始进行 WEIBOBASICINFO")

        try:
            mongo_data_list = m_client.all_from_mongodb(collection=collection)
        except pymongo.errors.ServerSelectionTimeoutError:
            time.sleep(1)
            mongo_data_list = m_client.all_from_mongodb(collection=collection)

        # 清洗数据并插入 HBase
        if mongo_data_list:
            self.find_count = mongo_data_list.count()
            for data in mongo_data_list:
                re_data = ""
                data_id = data["_id"]
                copy_data = {}
                self.remove_id_list.append(data_id)
                try:
                    del data["_id"]
                    copy_data = deepcopy(data)
                    self.copy_mongo_data_list.append(copy_data)
                    re_data = self.data_shuffle(data=data, province_list=province_list)
                    if not re_data:
                        self.bad_count += 1
                        continue
                except Exception as e:
                    self.remove_id_list.remove(data_id)
                    self.copy_mongo_data_list.remove(copy_data)
                    self.logger.warning("清洗错误,错误 _id 为{}, {}".format(data_id, e))

                # phoenix_HBase 插入数据
                try:
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
    main = WeiboBasicInfoScript()
    main.run()
