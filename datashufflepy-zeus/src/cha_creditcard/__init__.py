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

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-8])

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
        :param table_name: Hbase 表名
        :param collection_name: MongoDB 集合名
        :param entity_code:
        :return:
        """
        # hasattr判断对象是否包括属性
        if not hasattr(cls, "instance"):
            cls.instance = super(GenericScript, cls).__new__(cls)
            # phoenix connection
            cls.p_client = PhoenixHbase(table_name=table_name)
            cls.connection = cls.p_client.connect_to_phoenix()
            # MongoDB connection
            cls.m_client = MongoClient(mongo_collection=collection_name)
            cls.db, cls.collection_list = cls.m_client.client_to_mongodb()
            # MongoDB old connection
            cls.old_client = MongoClient(mongo_collection=collection_name)
            cls.old_client.mongo_db = "spider_data_old"
            cls.old_db, cls.old_collection_list = cls.old_client.client_to_mongodb()
            # Mysql connection
            cls.mysql_client, cls.mysql_connection = cls.mysql_connect(dev=True)
            cls.bank_list = cls.data_from_mysql()
            # Log
            cls.logger = Logger().logger
            # 统计
            cls.count_all = 0

        return cls.instance

    def __init__(self, table_name, collection_name, param, verify_field=None):
        """
        初始化 HBase 表名, MongoDB 集合名
        :param table_name: HBase 表名
        :param collection_name: MongoDB 集合名
        :param verify_field: 去重条件查询字段 dict
        :param param:
        """
        self.script_name = table_name
        self.p_client.table_name = table_name
        self.m_client.mongo_collection = collection_name
        self.old_client.mongo_collection = collection_name
        self.collection = self.m_client.get_check_collection(db=self.db, collection_list=self.collection_list)
        self.old_collection = self.old_client.get_check_collection(db=self.old_db,
                                                                   collection_list=self.old_collection_list)
        self.hbase_count = 0
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

    def verify_exist_in_data(self, data):
        """
        判断验证字段是否在数据中
        :return:
        """
        for value in self.verify_field.values():
            if value not in data:
                return False

        return True

    def exist_in_hbase(self, data, table_name=None):
        """
        判断数据是否在hbase中存在
        :param data:
        :return:
        """
        verify_condition = []
        verify_condition.extend(
            [f"{k} = '{value_replace(data[v])}'" for k, v in self.verify_field.items()])
        final_condition = " and ".join(verify_condition)
        result = self.p_client.search_all_from_phoenix(connection=self.connection, table_name=table_name,
                                                       output_field="count(1)",
                                                       iter_status=False, where_condition=final_condition)
        count = result[0][0].value
        # 重复返回 False
        if count:
            self.logger.info(f"重复值，未插入HBase:{verify_condition}")
        return count > 0

    def do_insert(self, fina_shuffle, network_status, remove_id):
        id_list = list()
        for each in fina_shuffle:
            # 如果验证字段在数据中，继续验证是否已经在hbase存在，否则直接插入
            if self.verify_exist_in_data(each['DATA_']):
                if not self.exist_in_hbase(each['DATA_'], each["TABLE_NAME_"]):
                    # 插入 HBase 和 MySQL
                    network_status = self.insert_data(fina_shuffle=each["DATA_"],
                                                      remove_id=remove_id,
                                                      table_name=each["TABLE_NAME_"])
            else:
                # 插入 HBase 和 MySQL
                network_status = self.insert_data(fina_shuffle=each["DATA_"],
                                                  remove_id=remove_id, table_name=each["TABLE_NAME_"])
            id_list.append(each["DATA_"]['ID_'])
        return network_status, id_list

    def generic_shuffle(self, data, re_data, field=None):
        """
        通用清洗规则写在这里, 现只有从字段中匹配银行。
        :param data: 要清洗的数据 type: dict
        :param re_data: 要清洗的数据 type: dict
        :param field: 要清洗的字段名 type: str: "CONTENT_" or "PRO_NAME_" or ...
                                          NoneType: None 无需清洗
        :return: 清洗完毕的数据 type: dict
        """

        # 涉及银行统一在 __init_____.py 中处理
        # if field:
        #     if "BANK_NAME_" not in re_data:
        #         for bank in self.bank_list:
        #             if data["ENTITY_NAME_"][:-4] in bank["ALIAS_"]:
        #                 re_data["BACK_CODE_"] = bank["CODE_"]  # 银行编码
        #                 re_data["BACK_NAME_"] = bank["NAME_"]  # 银行名称
        #                 break

        if "ID_" not in re_data:
            serial_number = req_for_serial_number(code=data["ENTITY_CODE_"][:8])
            re_data["ID_"] = serial_number
        # 文件上传
        if "YJBG_" in data["ENTITY_CODE_"]:
            tc = "YJBG"

        if data["FILE_URL_"]:
            re_postfix = re.findall(r"\.([pd][do][fc]x?$)", data["FILE_URL_"])
            if re_postfix or data.get('ENTITY_CODE_') in ['XYK_YJBG_GFYH', 'XYK_YJBG_JTYH']:
                postfix = re_postfix[0] if re_postfix else 'pdf'
                if "FILE_NAME_" in data:
                    file_name = data["FILE_NAME_"]
                else:
                    re_file_name = re.findall(rf"/(.*?)\.{postfix}", data["FILE_URL_"], re.IGNORECASE)
                    if re_file_name:
                        file_name = re_file_name[0]
                    else:
                        file_name = str(uuid.uuid1())
                try:
                    response = req_for_something(url=data["FILE_URL_"])
                except Exception as e:
                    self.logger.exception(f"2.1--err: PDF"
                                          f" 原始数据 collection = {self.m_client.mongo_collection};"
                                          f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                          f" 原始数据 _id = {data['_id']};"
                                          f"error: {e}.")
                else:
                    print('附件请求成功')
                    if response:
                        try:
                            # p_response = req_for_file_save(id=re_data["ID_"], type_code=f"CHA_{tc}_{postfix.upper()}",
                            p_response = req_for_file_save(id=re_data["ID_"], type_code=f"CHA_YJBG",
                                                           file_name=file_name, postfix=postfix,
                                                           file=response.content)
                            if "error" in p_response.content.decode("utf-8"):
                                self.logger.info(f"2.3--err:文件上传错误."
                                                 f" 原始数据collection={self.m_client.mongo_collection};"
                                                 f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                                 f" 原始数据 _id = {data['_id']};"
                                                 f" error: {p_response.content.decode('utf-8')}.")
                                raise Exception("上传文件出错")
                            else:
                                self.logger.info(f"2.3--success: 文件上传成功."
                                                 f"{p_response.content.decode('utf-8')}")
                            p_response.close()
                        except Exception as e:
                            self.logger.exception(f"2.1--err: PDF"
                                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                                  f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                                  f" 原始数据 _id = {data['_id']};"
                                                  f"error: {e}.")
                            raise Exception("上传文件出错")
                        finally:
                            response.close()
                    else:
                        self.logger.exception(f"2.1--err: PDF"
                                              f" 原始数据 collection = {self.m_client.mongo_collection};"
                                              f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                              f" 原始数据 _id = {data['_id']};"
                                              f"error: PDF 请求失败.")
                        raise Exception("文件请求失败")

        if "ENTITY_CODE_" not in re_data:
            re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        if "ENTITY_NAME_" not in re_data:
            re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        if "URL_" not in re_data:
            if "URL_" in data:
                re_data["URL_"] = data["URL_"]
        # 创建时间及操作人
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        re_data["CREATE_TIME_"] = create_time
        re_data["CREATE_BY_ID_"] = CREATE_ID
        re_data["CREATE_BY_NAME_"] = CREATE_NAME

        # 爬取时间
        if "DATETIME_" in data:
            re_data["SPIDER_TIME_"] = data["DATETIME_"]
        elif ("DATETIME_" not in data) and ("DEALTIME_" in data):
            d_time = arrow.get(data["DEALTIME_"])
            date_time = d_time.format("YYYY-MM-DD")
            re_data["SPIDER_TIME_"] = date_time
        if "PERIOD_CODE_" not in re_data:
            re_data["PERIOD_CODE_"] = re_data.get("PUBLISH_TIME_", "")
        if "M_STATUS_" not in re_data:
            re_data["M_STATUS_"] = "N"
        if "DELETE_STATUS_" not in re_data:
            re_data["DELETE_STATUS_"] = "N"
        if "DATA_STATUS_" not in re_data:
            re_data["DATA_STATUS_"] = "UNCHECK"
        if "VERSION_" not in re_data:
            re_data["VERSION_"] = "0"
        if "DATA_VERSION_" not in re_data:
            re_data["DATA_VERSION_"] = "0"
        if "MICROBLOG" not in re_data["ENTITY_CODE_"] and "PUBLISH_STATUS_" not in re_data:
            re_data["PUBLISH_STATUS_"] = "N"

        return re_data

    @classmethod
    def mysql_connect(cls, dev=False):
        if dev:
            mysql_config = {
                "host": MYSQL_HOST_41,
                "port": MYSQL_PORT_41,
                "database": MYSQL_DATABASE_41,
                "user": MYSQL_USER_41,
                "password": MYSQL_PASSWORD_41,
                "table": "cha_bank"
            }
            mysql_client = MysqlClient(**mysql_config)
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
        if not mysql_client:
            mysql_client = cls.mysql_client
        if not mysql_connection:
            mysql_connection = cls.mysql_connection
        # # 省级
        # province_list = mysql_client.search_from_mysql(connection=mysql_connection, where_condition="PARENT_ is null")
        # # 市级
        # city_list = mysql_client.search_from_mysql(connection=mysql_connection,
        #                                            where_condition='CODE_ REGEXP "[0-9][0-9][0-9][0-9]00"')
        # # 区县级
        # area_list = mysql_client.search_from_mysql(connection=mysql_connection,
        #                                            where_condition='PARENT_ REGEXP "[0-9][0-9][0-9][0-9]00"')
        # # 省直辖县级行政区划
        # dir_area_list = mysql_client.search_from_mysql(connection=mysql_connection,
        #                                                where_condition='CODE_ REGEXP "9[0-9][0-9][0-9]$"')
        # # 银行编码
        # mysql_client.mysql_table = "cha_bank"
        bank_list = mysql_client.search_from_mysql(connection=mysql_connection)
        # cls.mysql_client.close_client(connection=cls.mysql_connection)

        # return province_list, city_list, area_list, dir_area_list, bank_list
        return bank_list

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
        if "ENTITY_CODE_" not in data:
            shuffle_data = self.generic_shuffle(data=data)
        elif "WECHAT" not in data["ENTITY_CODE_"] and "WEIBO" not in data["ENTITY_CODE_"] and "MAPBAR" not in data[
            "ENTITY_CODE_"] and "MICROBLOG" not in data["ENTITY_CODE_"]:
            # 拼接导入文件路径
            if not self.script_path:
                path_name = data["ENTITY_CODE_"]
            else:
                path_name = ".".join([self.script_path, data["ENTITY_CODE_"]])

            # 导入对应文件
            module_name = __import__(path_name, fromlist=data["ENTITY_CODE_"])
            # 调用对应文件中的 data_shuffle 进行清洗
            try:
                if "ORGANIZE" in data["ENTITY_CODE_"]:
                    re_data = module_name.data_shuffle(data, self.province_list, self.city_list, self.area_list)
                else:
                    re_data = module_name.data_shuffle(data)
            except Exception as e:
                self.logger.exception(f"2.4--err:实体脚本错误."
                                      f" 原始数据 collection = {self.m_client.mongo_collection};"
                                      f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                      f" 原始数据 _id = {data['_id']};"
                                      f" error: {e}.")
                raise Exception(e)
            else:
                if re_data:
                    shuffle_data = self.generic_shuffle(data=re_data)
                else:
                    return
        else:
            shuffle_data = self.generic_shuffle(data=data)
        return shuffle_data

    def insert_data(self, fina_shuffle, remove_id, table_name):
        # 向 HBase 中插入一条
        try:
            p_count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=fina_shuffle,
                                                             table_name=table_name)
            self.logger.info(f"3--success:插入 HBase 成功."
                             f" 原始数据 collection = {self.m_client.mongo_collection};"
                             f" ENTITY_CODE_ = {fina_shuffle.get('ENTITY_CODE_', 'None')};"
                             f" 原始数据 _id = {remove_id};"
                             f" HBase数据 ID_ = {fina_shuffle['ID_']};")
            if p_count % 100 == 0:
                self.logger.info(
                    f"HBase 插入{self.p_client.table_name} 成功, _id: {remove_id}, 成功条数 {p_count}")
        except Exception as e:
            self.logger.exception(f"3.1--error: 插入 HBase 失败."
                                  f" 原始数据 collection = {self.m_client.mongo_collection}."
                                  f" ENTITY_CODE_ = {fina_shuffle.get('ENTITY_CODE_', 'None')}."
                                  f" 原始数据 _id = {remove_id}."
                                  f" error: {e}")
            self.p_client.count -= 1
            return

        # 向 MySQL 中插入一条
        try:
            network_status = self.count_network_volume(data=fina_shuffle)
            return network_status
        except Exception as e:
            self.logger.exception(f"MySQL 插入失败, _id: {remove_id}, error: {e}")
            self.p_client.delete_from_phoenix(connection=self.connection,
                                              where_condition=f'ID_ = \'{fina_shuffle["ID_"]}\'')
            return

    def close_client(self):
        if self.count_all < 0:
            self.count_all = 0
        self.logger.info(f"数据清洗完成, 成功条数共计: {self.count_all}")
        self.m_client.client_close()
        self.p_client.close_client_phoenix(connection=self.connection)
        self.mysql_client.close_client(connection=self.mysql_connection)
        # self.h_client.transport.close()

    def main(self):
        """
        主函数, 主逻辑(包含从 MongoDB 取出数据, 清洗, 插入 HBase)
        :return:
        """
        self.logger.info(f"1--接收到的参数: {str(self.param_dict)}")
        source_count = 0
        # 从 MongoDB 取出数据
        if not self.limit_number:
            self.limit_number = 500
        for i in range(self.limit_number):
            # 取一条处理 d=1表示正在处理，0或空表示待处理
            try:
                data = self.m_client.get_data_and_update(collection=self.collection, entity_code=self.entity_code,
                                                         exclude_code=self.exclude_code,
                                                         update_dict={"$set": {"d": DOING}},
                                                         other_query={'$or': [{'d': {'$exists': False}}, {'d': TO_DO}]},
                                                         sort_query=[('d',pymongo.ASCENDING)])
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(3)
                data = self.m_client.get_data_and_update(collection=self.collection, entity_code=self.entity_code,
                                                         exclude_code=self.exclude_code,
                                                         update_dict={"$set": {"d": DOING}},
                                                         other_query={'$or': [{'d': {'$exists': False}}, {'d': TO_DO}]},
                                                         sort_query=[('d',pymongo.ASCENDING)])
            self.logger.info(f"=" * 50)
            reset_data = deepcopy(data)
            if not data:
                break
            # 初始化网络升量状态
            network_status = False
            id_list = []
            if self.verify_exist_in_data(data):
                if not self.exist_in_hbase(data=data):
                    # 数据清洗
                    try:
                        fina_shuffle = self.shuffle_data(data=data)
                        self.logger.info(f"2--转换成功."
                                         f" 原始数据 collection = {self.m_client.mongo_collection};"
                                         f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                         f" 原始数据 _id = {data['_id']}.")
                    except Exception as e:
                        self.logger.exception(f"数据清洗出错, _id: {data['_id']}, error: {e}")
                        ex_type, ex_val, ex_stack = sys.exc_info()
                        erro = f"{ex_type} \n{ex_val}"
                        for stack in traceback.extract_tb(ex_stack):
                            erro+=f"\n{stack}"
                        self.m_client.update_to_mongodb(self.collection,data['_id'],{'d':ERRO,'shuffleErro':str(erro)})
                        continue
                    if not fina_shuffle:
                        self.m_client.update_to_mongodb(self.collection, data['_id'],
                                                        {'d': -1, 'shuffleErro': "bad data"})
                        continue
                    network_status, id_list = self.do_insert(fina_shuffle, network_status, data['_id'])
            else:
                # 校验字段不在数据中，需要清洗后校验
                try:
                    fina_shuffle = self.shuffle_data(data=data)
                    self.logger.info(f"2--转换成功."
                                     f" 原始数据 collection = {self.m_client.mongo_collection};"
                                     f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                     f" 原始数据 _id = {reset_data['_id']}.")
                except Exception as e:
                    ex_type, ex_val, ex_stack = sys.exc_info()
                    erro = f"{ex_type} \n{ex_val}"
                    for stack in traceback.extract_tb(ex_stack):
                        erro += f"\n{stack}"
                    self.m_client.update_to_mongodb(self.collection, data['_id'], {'d': ERRO, 'shuffleErro': str(erro)})
                    self.logger.exception(f"数据清洗出错, _id: {data['_id']} 更改mongo状态, error: {e}")
                    continue
                if not fina_shuffle:
                    self.m_client.update_to_mongodb(self.collection, data['_id'],
                                                    {'d': -1, 'shuffleErro': "bad data"})
                    continue
                network_status, id_list = self.do_insert(fina_shuffle, network_status, data['_id'])
            if "微博" in data["ENTITY_NAME_"]:
                continue
            if not network_status:
                continue
            # 4. 插入备份数据
            try:
                time_array = time.localtime()
                shuffle_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
                reset_data["SHUFFLE_TIME_"] = shuffle_time
                del reset_data["_id"]
                self.old_client.all_to_mongodb(collection=self.old_collection, insert_list=[reset_data])
                self.logger.info(f"4--success:插入备份数据库成功."
                                 f" 原始数据 collection = {self.m_client.mongo_collection}."
                                 f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                 f" 原始数据 _id = {data['_id']};"
                                 f" HBase 数据 ID_ = {' '.join(id_list)};"
                                 f" 备份数据 _id = {reset_data['_id']}.")
            except Exception as e:
                self.m_client.update_to_mongodb(self.collection, data['_id'], {'d': TO_DO})
                # 如果备份数据失败，删除phoenix数据
                self.logger.exception(f"4.1--err:插入备份数据失败. 更改mongo状态"  
                                      f" 原始数据 collection = {self.m_client.mongo_collection}."
                                      f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')}."
                                      f" 原始数据 _id = {data['_id']}."
                                      f" error: {e}.")
                if id_list:
                    self.p_client.delete_from_phoenix(connection=self.connection,
                                                      where_condition=f'ID_ in {tuple(id_list)}')
                    self.p_client.count -= len(id_list)
                    self.logger.info(f"4.2--rollback: 删除 HBase 数据成功 ID_ in {tuple(id_list)}.")
                    if isinstance(network_status, str):
                        for e in ["CJXW", "BDXW", "GWDT"]:
                            if e in reset_data["ENTITY_CODE_"]:
                                self.mysql_client.mysql_table = "cha_network_volume"
                                self.mysql_client.delete_from_mysql(connection=self.mysql_connection,
                                                                    where_condition=f'ID_ in {tuple(id_list)}')
                            break

            # 5. 删除 MongoDB 源数据
            # else:
            #     try:
            #         delete_count = self.m_client.remove_from_mongo(collection=self.collection,
            #                                                        remove_id_list=[data["_id"]])
            #         self.delete_count += delete_count
            #         self.logger.info(f"5--success: 删除原始数据成功."
            #                          f" 原始数据 collection = {self.m_client.mongo_collection};"
            #                          f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
            #                          f" 原始数据 _id = {data['_id']}.")
            #         if self.delete_count % 10 == 0:
            #             self.logger.info(f"MongoDB 删除成功, 成功条数 {self.delete_count}")
            #     except Exception as e:
            #         self.m_client.update_to_mongodb(self.collection, data['_id'], {'d': TO_DO})
            #         if id_list:
            #             # 如果删除失败，删除phoenix数据，和备份数据
            #             self.logger.exception(f"5.1--err: 删除原始数据失败."
            #                                   f" 原始数据 collection = {self.m_client.mongo_collection};"
            #                                   f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
            #                                   f" 原始数据 _id = {data['_id']};"
            #                                   f" HBase ID_ = {' '.join(id_list)};"
            #                                   f" error: {e}.")
            #             self.old_client.remove_from_mongo(collection=self.old_collection,
            #                                               remove_id_list=[ObjectId(reset_data["_id"])])
            #             self.logger.info(f"5.2--rollback: 删除备份库数据成功."
            #                              f" 原始数据 collection = {self.m_client.mongo_collection};"
            #                              f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
            #                              f" 原始数据 _id = {data['_id']};"
            #                              f" 备份数据 _id = {reset_data['_id']}.")
            #             self.p_client.delete_from_phoenix(connection=self.connection,
            #                                               where_condition=f'ID_ in {tuple(id_list)}')
            #             self.p_client.count -= 1
            #             self.logger.info(f"5.3--rollback: 删除 HBase 数据成功."
            #                              f" HBase ID_ = {' '.join(id_list)}.")
            #             if isinstance(network_status, bool):
            #                 if data["SOURCE_TYPE_"] in ["CJXW", "BDXW", "GWDT"]:
            #                     self.mysql_client.mysql_table = "cha_network_volume"
            #                     self.mysql_client.delete_from_mysql(connection=self.mysql_connection,
            #                                                         where_condition=f'ID_ in {tuple(id_list)}')
            #     else:
            #         self.logger.info(f"6--success(ALL): 数据清洗成功."
            #                          f" 原始数据 collection = {self.m_client.mongo_collection};"
            #                          f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
            #                          f" 原始数据 _id = {data['_id']};"
            #                          f" HBase 数据 ID_ = {' '.join(id_list)};"
            #                          f" 备份数据 _id = {reset_data['_id']}.")
            #         source_count += 1
            #         if source_count % 10 == 0:
            #             self.logger.info(f"########数据处理成功collection{self.old_client.mongo_collection} "
            #                              f"entitycode={data['ENTITY_CODE_']}, 成功条数 {source_count}")

        if self.p_client.count < 0:
            self.p_client.count = 0
        self.logger.info(f"****mongo数据, 成功条数共计: {source_count} 条")
        self.logger.info(f"****collection: {self.m_client.mongo_collection} 的数据清洗完毕, 成功条数共计: {self.p_client.count} 条")
        self.count_all += self.p_client.count
        self.p_client.count = 0
