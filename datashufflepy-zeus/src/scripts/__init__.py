# -*- coding: utf-8 -*-
"""通用脚本"""
import sys
import os
import time

import pymongo


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-8])

from log.data_log import Logger
from __config import *
from database._mongodb import MongoClient
from database._mysql import MysqlClient
from database._phoenix_hbase import PhoenixHbase


class GenericScript(object):
    def __new__(cls, table_name, collection_name):
        """

        :param table_name: Hbase 表名
        :param collection_name: MongoDB 集合名
        :return:
        """
        if not hasattr(cls, "instance"):
            cls.instance = super(GenericScript, cls).__new__(cls)

            # phoenix connection
            cls.p_client = PhoenixHbase(table_name=table_name)
            cls.connection = cls.p_client.connect_to_phoenix()
            # MongoDB connection
            cls.m_client = MongoClient(mongo_collection=collection_name)
            cls.db, cls.collection_list = cls.m_client.client_to_mongodb()
            # Mysql connection
            cls.province_list, cls.city_list, cls.area_list, cls.dir_area_list = cls.area_from_mysql()
            # Log
            cls.logger = Logger().logger
            # 银行字典
            cls.bank_dict = {'中国工商银行': 'ICBC', '中国农业银行': 'ABC', '中国银行': 'BOC', '中国建设银行': 'CCB', '交通银行': 'BOCOM',
                             '中国邮政储蓄银行': 'PSBC', '浙商银行': 'CZB', '渤海银行': 'CBHB', '中信银行': 'ECITIC', '中国光大银行': 'CEB',
                             '华夏银行': 'HXB', '中国民生银行': 'CMBC', '招商银行': 'CMB', '兴业银行': 'CIB', '广发银行': 'CGB',
                             '平安银行': 'PAB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL'}

        return cls.instance

    def __init__(self, table_name, collection_name):
        """

        :param table_name: Hbase 表名
        :param collection_name: MongoDB 集合名
        """
        self.script_name = table_name
        self.p_client.table_name = table_name
        self.m_client.mongo_collection = collection_name
        self.collection = self.m_client.get_check_collection(db=self.db, collection_list=self.collection_list)

    def generic_shuffle(self, data):
        """
        通用清洗规则写在这里, 现只有从 内容中匹配地区、银行。
        :param data: 要清洗的数据 type: dict
        :return: 清洗完毕的数据 type: dict
        """
        # 地区及经纬度清洗
        prov_list = list()
        prov_code_list = list()
        city_list = list()
        city_code_list = list()
        lat_list = list()
        lng_list = list()
        for city in self.city_list:
            if city["PARENT_"] in ["1100", "3100", "1200", "5000"]:
                continue
            elif city["NAME_"][:2] in data["CONTENT_"]:
                city_list.append(city["NAME_"])
                city_code_list.append(city["CODE_"])
                prov_code_list.append(city["PARENT_"])
                lat_list.append(city["LAT_"])
                lng_list.append(city["LNG_"])
        for prov in self.province_list:
            if prov["NAME_"][:2] in data["CONTENT_"]:
                prov_list.append(prov["NAME_"])
                prov_code_list.append(prov["CODE_"])
                lat_list.append(prov["LAT_"])
                lng_list.append(prov["LNG_"])
            elif prov["CODE_"] in prov_code_list:
                prov_list.append(prov["NAME_"])

        if prov_list:
            data["PROVINCE_CODE_"] = "|".join(prov_code_list)
            data["PROVINCE_NAME_"] = "|".join(prov_list)
        if city_list:
            data["CITY_CODE_"] = "|".join(city_code_list)
            data["CITY_NAME_"] = "|".join(city_list)
            # copy_result["AREA_CODE_"] = result[""]
            # copy_result["AREA_NAME_"] = result[""]
        if lat_list:
            data["LAT_"] = "|".join(lat_list)
            data["LNG_"] = "|".join(lng_list)

        # 涉及银行统一在 __init_____.py 中处理
        bank_list = list()
        bank_code_list = list()
        for key in self.bank_dict:
            if key in data["CONTENT_"]:
                bank_list.append(key)
                bank_code_list.append(self.bank_dict[key])
        if bank_list:
            data["BANK_NAME_"] = "|".join(bank_list)
        if bank_code_list:
            data["BANK_CODE_"] = "|".join(bank_code_list)

        # todo 机构
        # data["UNIT_CODE_"] = ""
        # data["UNIT_NAME_"] = ""

        return data

    @classmethod
    def data_from_mysql(cls):
        """
        查询地区编码和经纬度
        :return:
        """

        mysql_config = {
            "host": MYSQL_HOST_25,
            "port": MYSQL_PORT_25,
            "database": MYSQL_DATABASE_25,
            "user": MYSQL_USER_25,
            "password": MYSQL_PASSWORD_25,
            "table": "cha_di_position"
        }

        mysql_client = MysqlClient(**mysql_config)
        connection = mysql_client.client_to_mysql()
        # 省级
        province_list = mysql_client.search_from_mysql(connection=connection, where_condition="PARENT_ is null")
        # 市级
        city_list = mysql_client.search_from_mysql(connection=connection,
                                                   where_condition='CODE_ REGEXP "[0-9][0-9][0-9][0-9]00"')
        # 区县级
        area_list = mysql_client.search_from_mysql(connection=connection,
                                                   where_condition='PARENT_ REGEXP "[0-9][0-9][0-9][0-9]00"')
        # 省直辖县级行政区划
        dir_area_list = mysql_client.search_from_mysql(connection=connection,
                                                       where_condition='CODE_ REGEXP "9[0-9][0-9][0-9]$"')
        # 银行编码
        mysql_client.mysql_table = "cha_bank"
        bank_list = mysql_client.search_from_mysql(connection=connection)
        mysql_client.close_client(connection=connection)

        return province_list, city_list, area_list, dir_area_list, bank_list

    def main(self):
        """

        :return:
        """
        # get data from MongoDB
        mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection)
        while "不加班":
            try:
                data = mongo_data_list.__next__()
            except StopIteration:
                break
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(3)
                data = mongo_data_list.__next__()

            # shuffle data
            try:
                module_name = __import__(data["ENTITY_CODE_"])
                re_data = module_name.data_shuffle(data)
                if re_data:
                    shuffle_data = self.generic_shuffle(data=re_data)
                else:
                    continue
            except Exception as e:
                self.logger.exception(f"数据清洗出错, _id: {data['_id']}, error: {e}")
                continue
            # upsert to phoenix
            try:
                self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=shuffle_data)
            except Exception as e:
                self.logger.exception(f"HBase 插入失败, _id: {data['_id']}, error: {e}")
                continue
