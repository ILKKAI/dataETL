# -*- coding: utf-8 -*-
"""网点"""
import csv
import hashlib
import sys
import os
from copy import deepcopy
import urllib3
from bson import ObjectId
from scripts import GenericScript
from tools.web_api_of_baidu import get_lat_lng, get_area
from tools.web_api_of_gaode import gaode_get_lat_lng

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])

from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from time import sleep, time
from log.data_log import Logger
import pymongo


class AllToPhoenix(object):
    def __init__(self):
        # "ABCORGANIZE", "BOCOMORGANIZE","BOCORGANIZE", "CBHBORGANIZE", "CCBORGANIZE", "CEBORGANIZE",
        #                   "CGBORGANIZE", "CIBORGANIZE", "CMBCORGANIZE", "CMBORGANIZE", "CZBORGANIZE", "EBCLORGANIZE",
        self.code_list = [
                          "ECITICORGANIZE", "HXBORGANIZE", "ICBCORGANIZE", "PABORGANIZE", "PSBCORGANIZE",
                          "SPDBORGANIZE"]

        self.logger = Logger().logger
        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.copy_mongo_data_list = list()
        self.remove_id_list = list()
        self.branch_code_list = list()

        self.verify_list = ["ID_", "BANK_CODE_", "BANK_NAME_", "CREATE_TIME_", "AREA_CODE_", "UNIT_CODE_", "ADDR_",
                            "PROVINCE_NAME_", "PROVINCE_CODE_", "CITY_", "CITY_CODE_", "DISTRICT_NAME_",
                            "DISTRICT_CODE_", "LAT_", "LNG_", "NAME_", "ENTITY_CODE_", "DEALTIME_", "URL_",
                            "TEL_", "CODE_", "BUSINESS_HOURS_", "STATUS_1"]

    # 从 MongoDB 获取数据
    def get_data_from_mongo(self, m_client, collection, entity_code, data_id):
        m_client.mongo_db = "spider_data"
        m_client.mongo_entity_code = entity_code

        if data_id:
            data_id_obj = ObjectId(data_id)
        else:
            data_id_obj = None

        try:
            mongo_data_list = m_client.search_from_mongodb(collection, data_id=data_id_obj)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            sleep(1)
            mongo_data_list = m_client.search_from_mongodb(collection, data_id=data_id_obj)
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

    # 清洗经纬度和 AREA_CODE_
    def shuffle_for_area(self, re_data):
        if "STATUS_1" in re_data:
            del re_data["STATUS_1"]
            re_data["STATUS_"] = "1"
        location_result = get_lat_lng(re_data["ADDR_"])
        if location_result["status"] == 0:
            re_data["LNG_"] = str(location_result["result"]["location"]["lng"])
            re_data["LAT_"] = str(location_result["result"]["location"]["lat"])
            address_result = get_area(lat_lng=re_data["LAT_"]+","+re_data["LNG_"])
            # todo use formatted_address or not
            re_data["DISTRICT_NAME_"] = address_result["result"]["addressComponent"]["district"]
            re_data["DISTRICT_CODE_"] = address_result["result"]["addressComponent"]["adcode"]
            re_data["AREA_CODE_"] = address_result["result"]["addressComponent"]["adcode"]
            re_data["CITY_"] = address_result["result"]["addressComponent"]["city"]
            re_data["CITY_CODE_"] = address_result["result"]["addressComponent"]["adcode"][:4]+"00"
            re_data["PROVINCE_NAME_"] = address_result["result"]["addressComponent"]["province"]
            re_data["PROVINCE_CODE_"] = address_result["result"]["addressComponent"]["adcode"][:2]+"00"
            # todo use formatted_location or not
            # re_data["LNG_"] = lng_lat.split(",")[0]
            # re_data["LAT_"] = lng_lat.split(",")[1]
        else:
            re_data["LNG_"] = ""
            re_data["LAT_"] = ""

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
        # print("*"*150)
        # print(re_data)
        return re_data

    # 主函数
    def run(self):
        # 创建 Phoenix 对象
        p_client = PhoenixHbase(table_name="ORGANIZE_FINASSIST")
        p_client.verify_list = self.verify_list
        # 连接 Phoenix
        connection = p_client.connect_to_phoenix()
        # 创建 MongoDB 查询数据库对象
        m_client = MongoClient(mongo_collection="ORGANIZE_FINASSIST")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)
        # # 创建 MongoDB spider_data_old 数据库对象
        # old_client = MongoClient(mongo_collection="ORGANIZE_FINASSIST")
        # # 本地测试
        # old_client.client = pymongo.MongoClient(host="localhost", port=27017, serverSelectionTimeoutMS=60,
        #                                         connectTimeoutMS=60, connect=False)
        # old_client.mongo_db = "spider_data_old"
        # db_old, collection_list_old = old_client.client_to_mongodb()
        # collection_old = db_old["ORGANIZE_FINASSIST"]

        # 获取地区编码
        province_list, city_list, area_list, dir_area_list = (
            GenericScript(entity_code=None, entity_type="ORGANIZE_FINASSIST").area_from_mysql())

        # # 删除表
        # p_client.drop_table_phoenix(connection=connection)

        # # 创建表
        # # 网点表创建语句
        # sql = ('create table "ORGANIZE_FINASSIST" ("ID_" varchar primary key, "C"."BANK_NAME_" varchar,'
        #        '"C"."BANK_CODE_" varchar, "C"."NAME_" varchar,'
        #        '"C"."CODE_" varchar, "C"."ENTITY_NAME_" varchar, "C"."ENTITY_CODE_" varchar,'
        #        '"C"."AREA_CODE_" varchar, "C"."UNIT_CODE_" varchar, "C"."ADDR_" varchar,'
        #        '"C"."PROVINCE_NAME_" varchar, "C"."PROVINCE_CODE_" varchar, "C"."CITY_" varchar,'
        #        '"C"."CITY_CODE_" varchar, "C"."DISTRICT_NAME_" varchar, "C". "DISTRICT_CODE_" varchar,'
        #        '"C"."LAT_" varchar, "C"."LNG_" varchar, "C"."CREATE_TIME_" varchar, "C"."DEALTIME_" varchar,'
        #        '"C"."URL_" varchar, "C"."TEL_" varchar, "C"."BUSINESS_HOURS_" varchar, "C"."STATUS_" varchar,'
        #        '"C"."IMPORTANCE_" varchar) IMMUTABLE_ROWS = true')
        #
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
            # find_id = ""
            if entity_code == "ECITICORGANIZE":
                find_id = "5c3f48479bb3df1d97d762e1"
            else:
                find_id = None
            try:
                mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection,
                                                           entity_code=entity_code, data_id=find_id)
            except pymongo.errors.ServerSelectionTimeoutError:
                sleep(1)
                mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection,
                                                           entity_code=entity_code, data_id=find_id)

            # 清洗数据并插入 HBase
            if mongo_data_list:
                once_count = 0
                self.find_count = mongo_data_list.count()
                for data in mongo_data_list:
                    data_id = data["_id"]
                    # copy_data = {}
                    # self.remove_id_list.append(data_id)
                    try:
                        del data["_id"]
                        # copy_data = deepcopy(data)
                        # self.copy_mongo_data_list.append(copy_data)
                        re_data = module_name.data_shuffle(data, province_list, city_list, area_list)
                        if not re_data:
                            self.bad_count += 1
                            continue
                    except Exception as e:
                        # except jpype._jexception.SQLExceptionPyRaisable as e:
                        # except org.apache.phoenix.exception.BatchUpdateExecution as e:
                        # self.remove_id_list.remove(data_id)
                        # self.copy_mongo_data_list.remove(copy_data)
                        self.logger.exception("清洗错误,错误 _id 为{}, {}".format(data_id, e))
                        continue

                    print(data_id)

                    if isinstance(re_data, list):
                        for list_data in re_data:
                            area_data = ""
                            try:
                                # self.logger.info("_id {}".format(data_id))
                                area_data = self.shuffle_for_area(list_data)
                            except Exception as e:
                                # self.remove_id_list.remove(data_id)
                                # self.copy_mongo_data_list.remove(copy_data)
                                self.logger.exception("_id:{} 获取经纬度失败, {}".format(data_id, e))
                                continue
                            # except ValueError:
                            #     pass
                            # phoenix_HBase 插入数据
                            if area_data:
                                try:
                                    # print(area_data)
                                    success_count = p_client.upsert_to_phoenix_by_one(connection=connection,
                                                                                      data=area_data)
                                    once_count += success_count
                                    self.success_count += success_count
                                    # self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                                except Exception as e:
                                    # self.remove_id_list.remove(data_id)
                                    # self.copy_mongo_data_list.remove(copy_data)
                                    self.logger.exception("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                                    continue
                    elif isinstance(re_data, dict):
                        area_data = ""
                        try:
                            area_data = self.shuffle_for_area(re_data)
                        except urllib3.exceptions.NewConnectionError as e:
                            # self.remove_id_list.remove(data_id)
                            # self.copy_mongo_data_list.remove(copy_data)
                            self.logger.exception("_id: {}获取经纬度失败, {}".format(data_id, e))
                        except Exception as e:
                            # self.remove_id_list.remove(data_id)
                            # self.copy_mongo_data_list.remove(copy_data)
                            self.logger.exception("_id: {}获取经纬度失败, {}".format(data_id, e))
                            continue
                        # phoenix_HBase 插入数据
                        if area_data:
                            try:
                                # print(area_data)
                                success_count = p_client.upsert_to_phoenix_by_one(connection=connection, data=area_data)
                                once_count += success_count
                                self.success_count += success_count
                                # self.logger.info("HBase 插入成功, 成功条数 {} 条".format(success_count))
                            except Exception as e:
                                # self.remove_id_list.remove(data_id)
                                # self.copy_mongo_data_list.remove(copy_data)
                                self.logger.exception("HBase 插入 _id 为 {} 的数据失败, {}".format(data_id, e))
                                continue
                    if self.success_count % 100 == 0:
                        self.logger.info("HBase 插入成功, 成功条数 {} 条".format(self.success_count))
                    # 添加 {d:1}
                    # if self.success_count % 50 == 0:
                    #     update_count = m_client.update_to_mongodb(collection=collection,
                    #                                               data_id=self.remove_id_list,
                    #                                               data_dict={"d": 1})
                    #     self.remove_id_list = []
                    #     self.remove_count += update_count
                    #     self.logger.info("MongoDB 更新成功")

                mongo_data_list.close()

                # 添加 {d:1}
                # if self.remove_id_list:
                #     update_count = m_client.update_to_mongodb(collection=collection,
                #                                               data_id=self.remove_id_list,
                #                                               data_dict={"d": 1})
                #     self.remove_id_list = []
                #     self.remove_count += update_count
                #     self.logger.info("MongoDB 更新成功")
                if once_count > 0:
                    status = True
                    self.logger.info("HBase 插入成功, 成功条数 {}".format(once_count))
            else:
                continue
            # 删除数据
            # if status:
                # delete_count = self.delete_data_from_mongo(m_client=m_client, collection=collection,
                #                                            entity_code=entity_code,
                #                                            remove_id_list=self.remove_id_list)
                # self.remove_count += delete_count
                # self.logger.info("MongoDB 删除成功")
            # else:
            #     self.logger.info("HBase 插入成功条数0条, 不执行删除")

            # # 将数据插入 spider_data_old 中
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
