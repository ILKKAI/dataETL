# -*- coding: utf-8 -*-
import re
import time
import pymongo
import sys
import os


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath[:-15])
sys.path.append(rootPath[:-15])


from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from log.data_log import Logger
from tools.web_api_of_baidu import get_lat_lng


class MapBarTransfer(object):

    def __init__(self, table_name="CHA_BRANCH_MAPBAR", collection_name="mapbar"):
        # phoenix connection
        self.p_client = PhoenixHbase(table_name=table_name)
        self.connection = self.p_client.connect_to_phoenix()
        # MongoDB connection
        self.m_client = MongoClient(mongo_collection=collection_name, entity_code="MAPBAR_DEATAIL_BJ")
        self.m_client.mongo_host = "172.22.69.35"
        self.m_client.mongo_port = 20000
        self.m_client.client = pymongo.MongoClient(host="172.22.69.35", port=20000, serverSelectionTimeoutMS=60,
                                                   connectTimeoutMS=60, connect=False)
        self.db, self.collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=self.db, collection_list=self.collection_list)
        # Log
        self.logger = Logger().logger
        # count
        self.count = 0

    def main(self):
        # # 创建表
        # table_sql = (f'create table "{self.p_client.table_name}" ("ID_" varchar primary key,'
        #              '"C"."BTYPE_" varchar, "C"."TYPE_" varchar, "C"."NAME_" varchar, "C"."UPDATETIME_" varchar,'
        #              '"C"."ADDRESS_" varchar, "C"."POINAME_" varchar, "C"."PHONE_" varchar, "C"."BUSSTOP_" varchar,'
        #              '"C"."BUS_" varchar, "C"."URL_" varchar, "C"."DEALTIME_" varchar, "C"."DATETIME_" varchar,'
        #              '"C"."ENTITY_NAME_" varchar, "C"."ENTITY_CODE_" varchar, "C"."LAT_" varchar, "C"."LNG_" varchar'
        #              ') IMMUTABLE_ROWS = true')
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        # 获取数据
        # mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection)
        mongo_data_list = self.m_client.search_from_mongodb(collection=self.collection, field_name="DEALTIME_",
                                                            field_value={"$gt": "1555136656.0579224"},
                                                            data_id="5cb65fac9bb3df61a09c6625")

        count = 0
        while True:
            # 取一条处理
            try:
                data = mongo_data_list.__next__()
            except StopIteration:
                break
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(3)
                data = mongo_data_list.__next__()

            # 清洗
            try:
                data["PHONE_"] = data["PHONE_"].replace("无，", "")
                u_time_list = re.findall(r"(\d{4}年\d{1,2}月\d{1,2})日", data["UPDATETIME_"])
                if u_time_list:
                    u_ = u_time_list[0].replace("年", "-")
                    u_ = u_.replace("月", "-")
                    u_l = u_.split("-")
                    if len(u_l[1]) == 1:
                        u_l[1] = "0" + u_l[1]
                    if len(u_l[2]) == 1:
                        u_l[2] = "0" + u_l[2]
                    data["UPDATETIME_"] = "-".join(u_l)
            except Exception as e:
                self.logger.exception(f"数据清洗出错, _id: {data['_id']}, error {e}")
                continue

            # 获取经纬度
            try:
                if data["ADDRESS_"]:
                    data["ADDRESS_"] = "".join(data["ADDRESS_"].split("|")[1:])
                    location_result = get_lat_lng(address=data["ADDRESS_"])
                    if location_result["status"] == 0:
                        data["LNG_"] = str(location_result["result"]["location"]["lng"])
                        data["LAT_"] = str(location_result["result"]["location"]["lat"])
                    else:
                        self.logger.warning(f"_id: {data['_id']} 获取经纬度失败")
                else:
                    continue
            except Exception as e:
                self.logger.exception(f"_id: {data['_id']} 获取经纬度失败, error: {e}")
                continue
            # upsert to HBase
            try:
                re_data = self.__check_lat(data=data)
                # 向 HBase 中插入一条
                self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=re_data)
                count += 1
                if count % 100 == 0:
                    self.logger.info(f"HBase 插入成功, _id: {data['_id']}, 成功条数 {count}")
            except Exception as e:
                self.logger.exception(f"HBase 插入失败, _id: {data['_id']}, error: {e}")
                continue

        # 关闭 MongoDB cursor
        mongo_data_list.close()
        self.logger.info(f"collection: {self.m_client.mongo_collection} 的数据清洗完毕, 成功条数共计: {count} 条")

    def check_lat(self):
        # # 删除表
        # self.p_client.drop_table_phoenix(connection=self.connection, table_name="CHA_BRANCH_MAPBAR")
        #
        # table_sql = (f'create table "CHA_BRANCH_MAPBAR" ("ID_" varchar primary key,'
        #              '"C"."BTYPE_" varchar, "C"."TYPE_" varchar, "C"."NAME_" varchar, "C"."UPDATETIME_" varchar,'
        #              '"C"."ADDRESS_" varchar, "C"."POINAME_" varchar, "C"."PHONE_" varchar, "C"."BUSSTOP_" varchar,'
        #              '"C"."BUS_" varchar, "C"."URL_" varchar, "C"."DEALTIME_" varchar, "C"."DATETIME_" varchar,'
        #              '"C"."ENTITY_NAME_" varchar, "C"."ENTITY_CODE_" varchar, "C"."LAT_" varchar, "C"."LNG_" varchar,'
        #              '"C"."CHECK_LAT_" varchar, "C"."CHECK_LNG_" varchar'
        #              ') IMMUTABLE_ROWS = true')
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        self.p_client.table_name = "FANSILE"
        data_cursor = self.p_client.search_all_from_phoenix(connection=self.connection, dict_status=True)
        self.p_client.table_name = "CHA_BRANCH_MAPBAR"
        while True:
            try:
                data = data_cursor.__next__()

                # del data["('C', 'CHECK_LNG_')"]
                # if not data["LAT_"]:
                #     self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=data)
                #     continue
                # if 30.7083860773 < float(data["LAT_"]) < 31.8739003864:
                #     pass
                # else:
                #     self.logger.warning(f"错误 _id: {data['ID_']}, 经纬度: {data['LAT_']},{data['LNG_']}")
                #     data["CHECK_LAT_"] = data["LAT_"]
                #     data["CHECK_LNG_"] = data["LNG_"]
                #     data["LAT_"] = ""
                #     data["LNG_"] = ""
                #
                #     self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=data)
                #     continue
                # if 120.8778122800 < float(data["LNG_"]) < 122.1248433443:
                #     self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=data)
                #     continue
                # else:
                #     self.logger.warning(f"错误 _id: {data['ID_']}, 经纬度: {data['LAT_']},{data['LNG_']}")
                #     data["CHECK_LAT_"] = data["LAT_"]
                #     data["CHECK_LNG_"] = data["LNG_"]
                #     data["LAT_"] = ""
                #     data["LNG_"] = ""
                #     self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=data)　
                #     continue
                self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=data)
                self.count += 1
                if self.count % 100 == 0:
                    self.logger.info(f"HBase 插入成功, _id: {data['_id']}, 成功条数 {self.count} 条")

            except StopIteration:
                break

    def __check_lat(self, data):
        if "LAT_" not in data:
            return data
        # 上海
        # if 30.7083860773 < float(data["LAT_"]) < 31.8739003864:
        # 北京
        if 39.4498800000 < float(data["LAT_"]) < 41.1684980000:
            pass
        else:
            self.logger.warning(f"错误 _id: {data['_id']}, 经纬度: {data['LAT_']},{data['LNG_']}")
            data["CHECK_LAT_"] = data["LAT_"]
            data["CHECK_LNG_"] = data["LNG_"]
            data["LAT_"] = ""
            data["LNG_"] = ""
            return data
        # 上海
        # if 120.8778122800 < float(data["LNG_"]) < 122.1248433443:
        # 北京
        if 115.4534230000 < float(data["LNG_"]) < 117.5461160000:
            return data
        else:
            self.logger.warning(f"错误 _id: {data['_id']}, 经纬度: {data['LAT_']},{data['LNG_']}")
            data["CHECK_LAT_"] = data["LAT_"]
            data["CHECK_LNG_"] = data["LNG_"]
            data["LAT_"] = ""
            data["LNG_"] = ""
            return data


if __name__ == '__main__':
    script = MapBarTransfer()
    script.main()
    # script.check_lat()

    """
    上海
    north: 31.8739003864,121.3068218738
    south: 30.7083860773,121.6223621428
    west: 31.1271697869,120.8778122800
    east: 31.3158374894,122.1248433443
    shut down -s -t 8000
    """
    """
    北京
    north: 41.1684980000,116.7107640000
    south: 39.4498800000,116.4284800000
    west: 39.9517310000,115.4534230000
    east: 40.7072220000,117.5461160000
    """
    # column_arrary_list = [[('C', 'CHECK_LAT_'),('C', 'CHECK_LNG_')]]
    # column_sql_list = list()
    # for each_column in column_arrary_list:
    #     print(each_column)
    #     column_sql = f'"{each_column[0]}"."{each_column[1]}" VARCHAR'
    #     column_sql_list.append(column_sql)
    # print(column_sql_list)
    #
    # sql = f'ALTER TABLE "xxx" ADD {",".join(column_sql_list)}'
    # print(sql)
