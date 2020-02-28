# -*- coding: utf-8 -*-
"""通用脚本"""
import re
import sys
import os
import time
import traceback
import uuid
from copy import deepcopy
import arrow
import pymongo
from bson import ObjectId

from tools.req_for_wordExcelZip import find_type

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
src_dir = curPath + "/Files/"

from log.data_log import Logger
from __config import *
from database._mongodb import MongoClient
from database._mysql import MysqlClient
from database._phoenix_hbase import PhoenixHbase, value_replace
from tools.req_for_api import req_for_serial_number, req_for_something, req_for_file_save
from tools.req_for_ai import req_for_ner

# 待处理
TO_DO = 0
# 正在处理
DOING = 1
# 清洗出错
ERRO = -1


class GenericScript(object):
    
    def __new__(cls, table_name, collection_name, param, verify_field=None):
        """
        :param collection_name: MongoDB 集合名
        :param param:
        :return:
        """
        if not hasattr(cls, "instance"):
            cls.instance = super(GenericScript, cls).__new__(cls)

            # MongoDB connection, 查询数据的mongo连接
            cls.m_client = MongoClient(mongo_collection=collection_name)
            cls.db, cls.collection_list = cls.m_client.client_to_mongodb()
            # cls.old_client.mongo_db = "spider_data"
            # cls.old_client.client = pymongo.MongoClient(host="172.22.69.41", port=27017, serverSelectionTimeoutMS=60,
            #                                             connectTimeoutMS=60, connect=False)
            # # Mysql connection
            cls.mysql_client, cls.mysql_connection = cls.mysql_connect()
            cls.province_list, cls.city_list, cls.area_list, cls.dir_area_list, cls.bank_list = cls.data_from_mysql()
            # Log
            cls.logger = Logger().logger
            # 统计
            cls.count_all = 0
            # 银行字典
            # cls.bank_dict = {'中国工商银行': 'ICBC', '中国农业银行': 'ABC', '中国银行': 'BOC', '中国建设银行': 'CCB', '交通银行': 'BOCOM',
            #                  '中国邮政储蓄银行': 'PSBC', '浙商银行': 'CZB', '渤海银行': 'CBHB', '中信银行': 'ECITIC', '中国光大银行': 'CEB',
            #                  '华夏银行': 'HXB', '中国民生银行': 'CMBC', '招商银行': 'CMB', '兴业银行': 'CIB', '广发银行': 'CGB',
            #                  '平安银行': 'PAB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL'}
            # 汉字阿拉伯字典
            cls.number_dict = {"〇": "0", "○": "0", "零": "0", "一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
                               "六": "6", "七": "7", "八": "8", "九": "9", "十": "10", "年": "-", "月": "-", "日": ""}
        return cls.instance

    def __init__(self, table_name, collection_name, param, verify_field=None):
        """
        初始化 HBase 表名, MongoDB 集合名
        :param collection_name: MongoDB 集合名
        :param verify_field: 去重条件查询字段 dict
        :param param:
        """
        self.script_name = table_name

        self.m_client.mongo_collection = collection_name
        self.collection = self.m_client.get_check_collection(db=self.db, collection_list=self.collection_list)
        self.delete_count = 0
        self.source_count = 0
        self.script_path = ""
        self.param_dict = eval(param)
        self.verify_field = verify_field
        self.shuffle_status = True

        try:
            self.entity_code = self.param_dict["entityCode"]
        except KeyError:
            self.entity_code = None
        try:
            self.exclude_code = self.param_dict["excludeCode"]
        except KeyError:
            self.exclude_code = None
        try:
            self.limit_number = int(self.param_dict["limitNumber"])
        except KeyError:
            self.limit_number = None

    def value_replace(self, value):
        if (value == "None") or (value is None):
            value = ""
            return value
        value = str(value).replace("[", "(")
        value = re.sub(r"[^~·`!！@#$￥%^…&*()（）_—\-+={}\[\]【】》《、:：；;\'\"‘’“”<>《》,，.。/?？| \w]", "", value)
        value = value.replace("]", ")")
        value = value.replace("\'", "\"")
        return value

    def verify_exist_in_data(self, data):
        """
        判断验证字段是否在数据中
        :return:
        """
        for value in self.verify_field.values():
            if value not in data:
                return False

        return True

    def generic_shuffle(self, data, re_data, field="CONTENT_"):

        re_data = deepcopy(data)

        # 文件存储
        for _ in range(1, 10):
            if f"FJ{_}_NAME_" in data and data.get(f'FJ{_}_URL_'):
                type = find_type(data.get(f'FJ{_}_URL_')) if find_type(data.get(f'FJ{_}_URL_')) else find_type(data.get(f"FJ{_}_NAME_"))
                if not type:
                    return re_data
                try:
                    response = req_for_something(url=data[f'FJ{_}_URL_'])
                except Exception as e:
                    self.logger.exception('文件获取出错')
                else:
                    if response:
                        try:
                            # todo 文件上传出错是否继续还是跳过
                            number = 3932
                            serial_number = req_for_serial_number(code="GOV_ZX_GDS")

                            file_name = src_dir + str(int(serial_number[5:13]) - number) + '-' + data.get(f"FJ{_}_NAME_").replace('.xlsx', '').replace('.xls', '').replace('.doc', '').replace('.docx', '').replace('.zip', '').replace('.pdf', '').replace('.PDF', '') + type

                            re_data[f'FILE_NAME_{_}_'] = str(int(serial_number[5:13]) - number) + '-' + data.get(f"FJ{_}_NAME_").replace('.xlsx', '').replace('.xls', '').replace('.docx', '').replace('.doc', '').replace('.zip', '').replace('.pdf', '').replace('.PDF', '') + type
                            with open(file_name, 'wb+') as fp:
                                fp.write(response.content)
                            print('保存文件成功', '  ', re_data[f'FILE_NAME_{_}_'])
                        except Exception as e:
                            self.logger.exception(f"2.1--err: PDF"
                                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                                  f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                                  f"error: {e}.")
                        finally:
                            response.close()

        return re_data

    @classmethod
    def mysql_connect(cls, config=None):
        if config:
            mysql_client = MysqlClient(**config)
        else:
            mysql_client = MysqlClient()
        connection = mysql_client.client_to_mysql()

        return mysql_client, connection

    @classmethod
    def data_from_mysql(cls, mysql_client=None, mysql_connection=None):
        """
        查询地区编码和经纬度
        :param mysql_client:
        :param mysql_connection:
        :return:
        """
        if not mysql_client or not mysql_connection:
            mysql_client, mysql_connection = cls.mysql_connect()

        # 省级
        province_list = mysql_client.search_from_mysql(connection=mysql_connection, where_condition="PARENT_ is null")
        # 市级
        city_list = mysql_client.search_from_mysql(connection=mysql_connection,
                                                   where_condition='CODE_ REGEXP "[0-9][0-9][0-9][0-9]00"')
        # 区县级
        area_list = mysql_client.search_from_mysql(connection=mysql_connection,
                                                   where_condition='PARENT_ REGEXP "[0-9][0-9][0-9][0-9]00"')
        # 省直辖县级行政区划
        dir_area_list = mysql_client.search_from_mysql(connection=mysql_connection,
                                                       where_condition='CODE_ REGEXP "9[0-9][0-9][0-9]$"')
        # 银行编码
        mysql_client.mysql_table = "cha_bank"
        bank_list = mysql_client.search_from_mysql(connection=mysql_connection)
        # cls.mysql_client.close_client(connection=cls.mysql_connection)

        return province_list, city_list, area_list, dir_area_list, bank_list

    def count_network_volume(self, data):
        if "ENTITY_CODE_" not in data:
            return "T"
        else:
            if "ZX" not in data["ENTITY_CODE_"][:5]:
                return "T"
            if data["SOURCE_TYPE_"] not in ["CJXW", "BDXW", "GWDT"]:
                return "T"
            if "BANK_CODE_" not in data:
                return "T"

        insert_list = list()
        source_dict = {"WECHAT": "WECHAT", "WEIBO": "WEIBO", "CJXW": "FINANCE", "BDXW": "LOCALNEWS", "GWDT": "OFFICIAL"}
        # 保险 INSURANCE、基金 FUND、理财 FINANCING、信用卡 CREDIT
        type_dict = {"保险": "INSURANCE", "基金": "FUND", "理财": "FINANCING", "信用卡": "CREDIT"}
        bank_list = data["BANK_CODE_"].split("|")
        # 微信 WECHAT、微博 WEIBO、财经 FINANCE、本地新闻 LOCALNEWS、官网动态 OFFICIAL
        source = source_dict[data["SOURCE_TYPE_"]]
        publish_time = data["PUBLISH_TIME_"]
        source_id = data["ID_"]
        title = data["TITLE_"]

        for bank in bank_list:
            bank_code = bank
            # 保险 INSURANCE、基金 FUND、理财 FINANCING、信用卡 CREDIT
            for each in ["保险", "基金", "理财", "信用卡"]:
                insert_data = dict()
                type_ = each
                # count = data["CONTENT_"].count(each)
                if each in data["CONTENT_"]:
                    count = 1
                else:
                    count = 0

                if count == 0:
                    continue
                else:
                    insert_data["ID_"] = str(uuid.uuid1())
                    insert_data["SOURCE_"] = source
                    insert_data["PUBLISH_TIME_"] = publish_time
                    insert_data["SOURCE_ID_"] = source_id
                    insert_data["TITLE_"] = title
                    insert_data["BANK_CODE_"] = bank_code
                    insert_data["TYPE_"] = type_dict[type_]
                    insert_data["COUNT_"] = count
                    insert_list.append(insert_data)
        if insert_list:
            self.mysql_client.mysql_table = "cha_network_volume"
            self.mysql_client.insert_to_mysql(connection=self.mysql_connection, data=insert_list)
        return True

    def shuffle_data(self, data):
        '''
        清洗流程 -> 调用每个实体中的的清洗方法
        :param data: 需要清洗的数据
        :return:
        '''
        if "ENTITY_CODE_" not in data:
            shuffle_data = self.generic_shuffle(data=data)  # 如果子类有继承,则调用子类的generic_shuffle
        elif "WECHAT" not in data["ENTITY_CODE_"] and "WEIBO" not in data["ENTITY_CODE_"] and "MAPBAR" not in data[
            "ENTITY_CODE_"] and "MICROBLOG" not in data["ENTITY_CODE_"]:
            # 拼接导入文件路径, 子类可能会有指定 script_path
            if not self.script_path:
                path_name = data["ENTITY_CODE_"]
            else:
                path_name = ".".join([self.script_path, data["ENTITY_CODE_"]])

            # 导入对应文件
            module_name = __import__(path_name, fromlist=data["ENTITY_CODE_"])
            # 调用对应实体中的 data_shuffle 进行清洗
            try:
                if "ORGANIZE" in data["ENTITY_CODE_"] or 'CRMJPFX_WD' in data["ENTITY_CODE_"]:
                    re_data = module_name.data_shuffle(data, self.province_list, self.city_list, self.area_list)
                else:
                    re_data = module_name.data_shuffle(data)
            except Exception as e:
                self.logger.exception(f"2.4--err:实体脚本错误."
                                      f" 原始数据 collection = {self.m_client.mongo_collection};"
                                      f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                      f" error: {e}.")
                raise Exception(e)
            else:
                if re_data:
                    shuffle_data = self.generic_shuffle(data=re_data)  # 调用通用的清洗脚本, 子类继承则调用的为子类的
                else:
                    return
        else:
            shuffle_data = self.generic_shuffle(data=data)
        return shuffle_data

    def do_insert(self, fina_shuffle, network_status, remove_id):
        id_list = list()
        for each in fina_shuffle:
            network_status = self.insert_data(fina_shuffle=each["DATA_"], remove_id=remove_id, table_name=each["TABLE_NAME_"])
            id_list.append(each["DATA_"]['ID_'])
        return network_status, id_list

    def insert_data(self, fina_shuffle, remove_id, table_name):
        # 向 mongo 中更新一条
        try:
            query = {'_id': fina_shuffle.get('_id')}
            update_dict = {"$set": {"TYPE_": fina_shuffle.get('type'),
                                    "SUMMARY_": fina_shuffle.get('summary'),
                                    "ACCESSORY_": fina_shuffle.get('accessory'),
                                    "LOCATION_": fina_shuffle.get('location'),
                                    "ID_": fina_shuffle.get('ID_'),
                                    "FILE_NAME_1_": fina_shuffle.get('FILE_NAME_1_'),
                                    "FILE_NAME_2_": fina_shuffle.get('FILE_NAME_2_'),
                                    "FILE_NAME_3_": fina_shuffle.get('FILE_NAME_3_'),
                                    "FILE_NAME_4_": fina_shuffle.get('FILE_NAME_4_'),
                                    "FILE_NAME_5_": fina_shuffle.get('FILE_NAME_5_'),
                                    "FILE_NAME_6_": fina_shuffle.get('FILE_NAME_6_'),
                                    "FILE_NAME_7_": fina_shuffle.get('FILE_NAME_7_'),
                                    }}
            self.db.GOV_ZX_GDS_shufflepy_data.insert_many([fina_shuffle])
            # self.db.GOV_ZX_GDS.find_one_and_update(query, update_dict)

            self.logger.info(f"3--success:插入 mongo 成功.")
        except Exception as e:
            self.logger.exception(f"3.1--error: 插入 mongo 失败.")
            return

    def close_client(self):
        if self.count_all < 0:
            self.count_all = 0
        self.logger.info(f"数据清洗完成, 成功条数共计: {self.count_all}")
        self.m_client.client_close()
        self.mysql_client.close_client(connection=self.mysql_connection)
        # self.h_client.transport.close()

    def main(self):
        """
        主函数, 主逻辑(包含从 MongoDB 取出数据, 调用清洗方法, 插入 HBase,删除mongo数据)
        :return:
        """
        self.logger.info(f"1--接收到的参数: {str(self.param_dict)}")
        source_count = 0
        # 从 MongoDB 取出数据 -> mongo取数据 ,更新数据逻辑
        if not self.limit_number:
            self.limit_number = 500
        for i in range(self.limit_number):
            # 取一条处理 d=1表示正在处理，0或空表示待处理
            try:
                data = self.m_client.get_data_and_update(collection=self.collection, entity_code=self.entity_code,
                                                         exclude_code=self.exclude_code,
                                                         update_dict={"$set": {"d": DOING}},
                                                         other_query={'$or': [{'d': {'$exists': False}}, {'d': TO_DO}]},
                                                         sort_query=[('d', pymongo.ASCENDING)])
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(3)
                data = self.m_client.get_data_and_update(collection=self.collection, entity_code=self.entity_code,
                                                         exclude_code=self.exclude_code,
                                                         update_dict={"$set": {"d": DOING}},
                                                         other_query={'$or': [{'d': {'$exists': False}}, {'d': TO_DO}]},
                                                         sort_query=[('d', pymongo.ASCENDING)])
            self.logger.info(f"=" * 50)
            reset_data = deepcopy(data)
            if not data:
                break
            # 初始化网络升量状态
            network_status = False
            id_list = []
            # 数据清洗
            try:
                fina_shuffle = self.shuffle_data(data=data)
                self.logger.info(f"2--转换成功."
                                 f" 原始数据 collection = {self.m_client.mongo_collection};"
                                 f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};")
            except Exception as e:
                self.logger.exception(f"数据清洗出错, _id: {data['_id']}, error: {e}")
                ex_type, ex_val, ex_stack = sys.exc_info()
                erro = f"{ex_type} \n{ex_val}"
                for stack in traceback.extract_tb(ex_stack):
                    erro += f"\n{stack}"
                self.m_client.update_to_mongodb(self.collection, data['_id'],
                                                {'d': ERRO, 'shuffleErro': str(erro)})
                continue
            if not fina_shuffle:
                self.m_client.update_to_mongodb(self.collection, data['_id'], {'d': -1, 'shuffleErro': "bad data"})
                continue
            network_status, id_list = self.do_insert(fina_shuffle, network_status, data.get('_id'))
            source_count += 1

        self.logger.info(f"****mongo数据, 成功条数共计: {source_count} 条")

