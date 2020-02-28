# -*- coding: utf-8 -*-

import hashlib
import sys
import os
import requests
import json
import pandas as pd
from copy import deepcopy

from database._mysql import MysqlClient
from scripts import GenericScript
# from tools.web_api_of_baidu import get_lat_lng, get_area
# from tools.web_api_of_gaode import gaode_get_lat_lng

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath[:-8])
sys.path.append(rootPath[:-8])


from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from time import sleep, time
from log.data_log import Logger
import pymongo

URL_FOR_LAT_LNG = "https://restapi.amap.com/v3/geocode/geo"
URL_FOR_AREA = "https://restapi.amap.com/v3/geocode/regeo"
AK = "09d8c58e44834029245dae4478dfbaec"
OUTPUT = "json"


class FundScript(object):
    def __init__(self):
        self.code_list = ["STCNFUND","ABCFUND","CCBFUND","CITICFUND","ICBCFUND"]
        self.logger = Logger().logger
        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.copy_mongo_data_list = list()
        self.remove_id_list = list()
        self.branch_code_list = list()

        # 基金
        self.verify_list = ["ENTITY_CODE_", "ENTITY_NAME_", "URL_", "PERIOD_CODE_","STATUS_", "REMARK_",
                            "CREATE_TIME_", "UPDATE_TIME_",  "CODE_", "NAME_",
                            "FUND_NEW_VALUE_", "TOTAL_NEW_VALUE_", "FUND_OLD_VALUE_", "TOTAL_OLD_VALUE_",
                            "DAILY_RATE_","YEAR_REWARD_", "SUBS_STATUS_", "ATONEM_STATUS_","TYPE_", "ID_",
                            "NEWEST_VALUE_", "TOTAL_VALUE_", "POPULARITY_", "RATING_", "OLD_VALUE_", "UNIT_VALUE_",
                            "SCALE_", "ESTABLISH_DATE_", "RISK_LEVEL_", "BASE_INFO_", "YIELD_", "INVEST_",
                             "MONTH_RATE_", "QUARTER_RATE_", "HALF_YEAR_RATE_","HISTORY_RATE_","FUND_STATUS_","COMPANY_",
                            "SUBS_STATUS_CODE_", "TYPE_CODE_"]

    # 从 MongoDB 获取数据
    def get_data_from_mongo(self, m_client, collection, entity_code):
        m_client.mongo_db = "spider_data"
        m_client.mongo_entity_code = entity_code

        try:
            mongo_data_list = m_client.search_from_mongodb(collection)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            sleep(1)
            mongo_data_list = m_client.search_from_mongodb(collection)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None
        except KeyError as e:
            self.logger.info(e)
            return None

    # 从 MongoDB 删除数据
    def delete_data_from_mongo(self, m_client, collection, entity_code, remove_id_list):
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

        # 网点 CODE_
        hash_m = hashlib.md5()
        hash_m.update(re_data["ADDR_"].encode("utf-8"))
        hash_addr_ = hash_m.hexdigest()
        re_data["CODE_"] = re_data["BANK_CODE_"] + "_" + re_data["AREA_CODE_"] + "_" + hash_addr_
        # for i in range(1, 10000):
        #         branch_code = "ABC" + "_" + re_data["AREA_CODE_"] + "_" + "00000"
        #         branch_code = branch_code[:len(branch_code)-len(str(i))] + "{}".format(i)
        #         if branch_code in branch_code_list:
        #             continue
        #         else:
        #             branch_code_list.append(branch_code)
        #             break
        return re_data

    def gaode_get_lat_lng(self,address):
        url = URL_FOR_LAT_LNG + "?" + "key=" + AK + "&address=" + address
        # url = url + "?location={}&output=json&pois=1&ak={}".format(address, ak)
        response = requests.get(url)
        temp = json.loads(response.content)
        response.close()
        return temp['geocodes'][0]['location']

    def dict_from_mysql(self,dict_code):
        # 创建 MySQL 对象
        mysql_config = {
            "host": "172.22.67.25",
            "port": 3306,
            "database": "chabei",
            "user": "root",
            "password": "Code123!@#",
            "table": "sys_dict_item"
        }

        mysql_client = MysqlClient(**mysql_config)
        mysql_connection = mysql_client.client_to_mysql()
        result = mysql_client.search_area_code(
            sql="select DICT_CODE_,ITEM_LABEL_,ITEM_VALUE_ from sys_dict_item where DICT_CODE_=\'{}\'".format(
                dict_code), connection=mysql_connection)

        mysql_client.close_client(connection=mysql_connection)
        return result

    # 主函数
    def run(self):
        count = 0
        # # 创建 Phoenix 对象-注意表格名字
        p_client = PhoenixHbase(table_name="FUND")
        p_client.verify_list = self.verify_list
        # # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="JSFUND_CCBDATA")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)
        #查询省市区的编码列表
        # script = GenericScript(entity_code="ICBCFUND", entity_type="JSFUND_CCBDATA")
        # province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
        list_SUBS_STATUS = self.dict_from_mysql("FUND_SUBS_STATUS")
        list_TYPE = self.dict_from_mysql("FUND_TYPE")

        # # 删除表
        # p_client.drop_table_phoenix(connection=connection)

        # # 基金表创建语句
        # sql = ('create table "FUND" ("ID_" varchar primary key,"C"."ENTITY_CODE_" varchar,"C"."AREA_CODE_" varchar,'
        #     '"C"."BANK_CODE_" varchar,"C"."BANK_NAME_" varchar,"C"."UNIT_CODE_" varchar,"C"."PERIOD_CODE_" varchar,"C"."REMARK_" varchar,'
        #     '"C"."CREATE_TIME_" varchar,"C"."UPDATE_TIME_" varchar,"C"."STATUS_" varchar,"C"."CODE_" varchar,"C"."NAME_" varchar,'
        #     '"C"."FUND_OLD_VALUE_" varchar,"C"."TOTAL_OLD_VALUE_" varchar,"C"."FUND_NEW_VALUE_" varchar,"C"."TOTAL_NEW_VALUE_" varchar,'
        #     '"C"."INVEST_PERIOD_" varchar,"C"."DAILY_RATE_" varchar,"C"."YEAR_REWARD_" varchar,"C"."SUBS_STATUS_" varchar,'
        #     '"C"."ATONEM_STATUS_" varchar,"C"."TYPE_" varchar,"C"."NEWEST_VALUE_" varchar,"C"."TOTAL_VALUE_" varchar,'
        #     '"C"."POPULARITY_" varchar,"C"."RATING_" varchar,"C"."ENTITY_NAME_" varchar,"C"."OLD_VALUE_" varchar,'
        #     '"C"."UNIT_VALUE_" varchar,"C"."SCALE_" varchar,"C"."ESTABLISH_DATE_" varchar,"C"."RISK_LEVEL_" varchar,'
        #     '"C"."BASE_INFO_" varchar,"C"."YIELD_" varchar,"C"."INVEST_" varchar,"C"."MONTH_RATE_" varchar,'
        #     '"C"."QUARTER_RATE_" varchar,"C"."HALF_YEAR_RATE_" varchar,"C"."URL_" varchar,"C"."HISTORY_RATE_" varchar,'
        #     '"C"."FUND_STATUS_" varchar,"C"."COMPANY_" varchar,"C"."SUBS_STATUS_CODE_" varchar,"C"."TYPE_CODE_" varchar)IMMUTABLE_ROWS = true')
        #
        # # 创建表
        # p_client.create_new_table_phoenix(connection=connection, sql=sql)

        # 遍历 ENTITY_CODE_ 列表
        # self.code_list = ["ABCORGANIZE"]
        for entity_code in self.code_list:
            status = False
            module_name = __import__(entity_code)
            self.logger.info("开始进行 ENTITY_CODE_ {}".format(entity_code))

            self.remove_id_list = []
            self.copy_mongo_data_list = []
            self.branch_code_list = []
            try:
                mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection,
                                                           entity_code=entity_code)

            except pymongo.errors.ServerSelectionTimeoutError:
                sleep(1)
                mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection,
                                                           entity_code=entity_code)

            # 清洗数据并插入 HBase
            if mongo_data_list:
                once_count = 0
                self.find_count = mongo_data_list.count()
                for data in mongo_data_list:
                    data_id = data["_id"]
                    copy_data = {}
                    self.remove_id_list.append(data_id)
                    try:
                        del data["_id"]
                        copy_data = deepcopy(data)
                        self.copy_mongo_data_list.append(copy_data)
                        # re_data = module_name.data_shuffle(data_list,province_list, city_list, area_list,list_SUBS_STATUS,list_TYPE)
                        re_data = module_name.data_shuffle(data, list_SUBS_STATUS, list_TYPE)
                        # re_data = module_name.data_shuffle(data_list)

                        if not re_data:
                            self.bad_count += 1
                            continue
                    except Exception as e:
                        # except jpype._jexception.SQLExceptionPyRaisable as e:
                        # except org.apache.phoenix.exception.BatchUpdateExecution as e:
                        self.remove_id_list.remove(data_id)
                        self.copy_mongo_data_list.remove(copy_data)
                        self.logger.warning("清洗错误,错误 _id 为{}, {}".format(data_id, e))
                        continue

                    if isinstance(re_data, list):
                        for list_data in re_data:
                            # try:
                            #     area_data = self.shuffle_for_area(list_data)
                            # except Exception as e:
                            #     self.remove_id_list.remove(data_id)
                            #     self.copy_mongo_data_list.remove(copy_data)
                            #     self.logger.warning("_id:{} 获取经纬度失败, {}".format(data_id, e))
                            #     continue
                            # except ValueError:
                            #     pass
                            # phoenix_HBase 插入数据
                            if list_data:
                                try:
                                    count += 1
                                    print(count)
                                    # print(list_data)
                                    success_count = p_client.upsert_to_phoenix_by_one(connection=connection,
                                                                                      data=list_data)
                                    # 导出csv
                                    # pd.DataFrame(area_data).to_csv("E:\\NEWS_CLEAN_\\" + module_name+ ".csv")
                                    once_count += success_count
                                    self.success_count += success_count
                                    self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                                    if self.success_count % 50 == 0:
                                        update_count = m_client.update_to_mongodb(collection=collection,
                                                                                  data_id=self.remove_id_list,
                                                                                  data_dict={"d": 1})

                                        self.remove_count += update_count
                                        self.logger.info("MongoDB 更新成功")
                                except Exception as e:
                                    self.remove_id_list.remove(data_id)
                                    self.copy_mongo_data_list.remove(copy_data)
                                    self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                                    continue
                    elif isinstance(re_data, dict):
                        # try:
                        # area_data = self.shuffle_for_area(re_data)
                        # except Exception as e:
                        #     self.remove_id_list.remove(data_id)
                        #     self.copy_mongo_data_list.remove(copy_data)
                        #     self.logger.warning("_id: {}获取经纬度失败, {}".format(data_id, e))
                        #     continue
                        # phoenix_HBase 插入数据
                        if re_data:
                            try:
                                success_count = p_client.upsert_to_phoenix_by_one(connection=connection, data=re_data)
                                once_count += success_count
                                self.success_count += success_count
                                if self.success_count % 100 == 0:
                                    self.logger.info("HBase 插入成功, 成功条数 {} 条".format(self.success_count))
                                # 添加 {d:1}
                                if self.success_count % 50 == 0:
                                    update_count = m_client.update_to_mongodb(collection=collection,
                                                                              data_id=self.remove_id_list,
                                                                              data_dict={"d": 1})

                                    self.remove_count += update_count
                                    self.logger.info("MongoDB 更新成功")
                            except Exception as e:
                                self.remove_id_list.remove(data_id)
                                self.copy_mongo_data_list.remove(copy_data)
                                self.logger.warning("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                                continue
                if once_count > 0:
                    status = True
                    self.logger.info("HBase 插入成功, 成功条数 {}".format(once_count))
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
    script = FundScript()
    script.run()
