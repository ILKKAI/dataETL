# -*- coding: utf-8 -*-
import os
import sys
import threading

import jpype
import jaydebeapi
import re
from __config import *
from DBUtils.PooledDB import PooledDB


curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-9])

from log.data_log import Logger

src_dir = curPath[:-9] + "/jars"
pho_logger = Logger().logger

# '''
#  mongo中 DEALTIME_/DATETIME_ 字段没有插入到 hbase 中
# phoenix 没有对空串("")进行处理，所以结果仍为null。
# phoenix 对varchar 类型的null显示为空白，对数值类型的null显示为null。
# 未做断开连接重连的功能
# '''


def value_replace(value):
    if (value == "None") or (value is None):
        value = ""
        return value
    value = str(value).replace("[", "(")
    value = re.sub(r"[^~·`!！@#$￥%^…&*()（）_—\-+={}\[\]【】、:：；;\'\"‘’“”<>《》,，.。/?？| \w]", "", value)
    value = value.replace("]", ")")
    value = value.replace("\'", "\"")
    return value


class PhoenixHbase(object):

    def __init__(self, table_name):
        self.phoenix_client_jar = src_dir + "/phoenix-5.0.0-HBase-2.0-client.jar"
        args = '-Djava.class.path=%s' % self.phoenix_client_jar
        jvm_path = jpype.getDefaultJVMPath()
        jpype.startJVM(jvm_path, args)
        self.count = 0
        self.table_name = table_name
        self.position_dict = dict()

    def __connect_to_phoenix(self):
        connection = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                                        'jdbc:phoenix:storage01,storage02,crawler01:2181:/hbase',
                                        # 'jdbc:phoenix:zookeeper01,zookeeper02,zookeeper03:2181:/hbase',
                                        ['', ''],
                                        self.phoenix_client_jar)
        return connection

    def connect_to_phoenix(self):
        connection = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                                        'jdbc:phoenix:storage01,storage02,crawler01:2181:/hbase',
                                        # 'jdbc:phoenix:zookeeper01,zookeeper02,zookeeper03:2181:/hbase',
                                        {"user": '', "password": ""},
                                        self.phoenix_client_jar, {"hbase.client.keyvalue.maxsize": "52428800"})
        return connection


class PhoenixHbasePool(PhoenixHbase):
    """
        hbase数据库对象，负责产生数据库连接 ,
        此类中的连接采用连接池实现获取连接对象：conn = PhoenixHbasePool.getConn()
        每次使用结束, 释放连接对象; conn.close()或del conn 或 PhoenixHbasePool.phoenix_dispose()
        """
    # 连接池对象
    __pool = None
    _instance_lock = threading.Lock()

    def __init__(self, table_name=None,**kwargs):
        super(PhoenixHbasePool, self).__init__(table_name=None, **kwargs)  # 传入父类的实例化参数
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self.conn = self.__getConn()
        self.cursor = self.conn.cursor()
        self.table_name = table_name

    # def __new__(cls, *arg, **kwargs):
    #     '''
    #     单例模式的实现思路, 在 __new__ 动态的给类设置一个属性, 在每次实例化新的实例时判断属性是否存在,若存在直接返回单例, 不存在创建单例再返回
    #     :param arg:
    #     :param kwargs:
    #     :return:
    #     '''
    #     if not hasattr(PhoenixHbasePool, '_instance'):
    #         with PhoenixHbasePool._instance_lock:
    #             if not hasattr(PhoenixHbasePool, '_instance'):
    #                 PhoenixHbasePool._instance = object.__new__(cls)
    #     return PhoenixHbasePool._instance

    def __getConn(self):
        """
        @summary: 静态方法，从连接池中取出连接
        @return phoenix_hbasedb.connection
        """
        if PhoenixHbasePool.__pool is None:
            # creator 传入 phoenix 的产生连接池的函数
            __pool = PooledDB(creator=self.connect_to_phoenix,
                              mincached=1,
                              maxcached=1000,
                              )
            PhoenixHbasePool.__pool = __pool
        # 从连接池取DB连接
        return __pool.connection()

    def __search_iter(self, curs, output_field, dict_status, connection=None):

        if output_field:
            if len(output_field) == 1 and ("count" in output_field[0] or "COUNT" in output_field[0]):
                output_field = None
        while True:
            result = curs.fetchone()

            # 全部字段
            if self.position_dict:
                if result:
                    result_dict = dict()
                    for j in range(len(result)):
                        result_dict[self.position_dict[j + 1]] = result[j]
                    yield result_dict
                else:
                    curs.close()
                    if connection:
                        connection.close()
                    raise StopIteration("No item to fetch, all item is already done")
            # 部分字段
            elif output_field:
                if result:
                    if dict_status:
                        result_dict = dict()
                        for index_j, j in enumerate(output_field):
                            result_dict[j] = result[index_j]
                        yield result_dict
                    else:
                        yield result
                else:
                    curs.close()
                    if connection:
                        connection.close()
                    raise StopIteration("No item to fetch, all item is already done")
            else:
                if result:
                    yield result
                else:
                    curs.close()
                    if connection:
                        connection.close()
                    raise StopIteration("No item to fetch, all item is already done")

    def __get_coloumn(self, connection, table_name):
        curs = connection.cursor()
        curs.execute(f"SELECT ORDINAL_POSITION, COLUMN_NAME FROM SYSTEM.\"CATALOG\" WHERE TABLE_NAME = \'{table_name}\'")
        position_list = curs.fetchall()
        curs.close()

        for each in position_list:
            self.position_dict[each[0]] = each[1]

    def search_all_from_phoenix(self, connection=None, table_name=None, output_field=None, limit_num=None, offset_num=None,
                                iter_status=True, dict_status=False, where_condition=None):
        """
        查询
        :param connection: phoenix 连接对象
        :param table_name: phoenix 查询表名
        :param output_field: 输出值的字段，类型为 str 或 list
        :param limit_num: 输出条数
        :param offset_num: 跨度，如跳过前10条数据
        :param iter_status: 为 True 返回值为生成器对象; 为 False 返回值为数据列表(推荐数据量较小时或 limit_num 较小时用)
        :param dict_status: True 返回字典类型数据, 键为字段, 值为字段对应的值
        :param where_condition: where 条件 exp: "WHERE ENTITY_CODE_ = 'XXX'" , 等号后面的值必须为单引号
        :return: 生成器对象 或 数据列表
        """
        connection = PhoenixHbasePool.__pool.connection()
        if not table_name:
            table_name = self.table_name
        else:
            if table_name != self.table_name:
                self.position_dict = dict()

        if not output_field:
            if not self.position_dict and dict_status:
                self.__get_coloumn(connection=connection, table_name=table_name)
            sql = f"SELECT * FROM \"{table_name}\""
        elif isinstance(output_field, (str, list)):
            self.position_dict = dict()
            if isinstance(output_field, str):
                output_field = [output_field]
            sql = f"SELECT {','.join(output_field)} FROM \"{table_name}\""
        else:
            return "param \"output_field\" need str or list"

        if where_condition:
            if ("where" in where_condition[:5]) or ("WHERE" in where_condition[:5]):
                where_condition = where_condition.replace("where", "", 1)
                where_condition = where_condition.replace("WHERE", "", 1)
            sql = f"{sql} WHERE {where_condition}"

        if limit_num:
            sql = sql + f" LIMIT {limit_num}"

        if offset_num:
            sql = sql + f" OFFSET {offset_num}"
        pho_logger.info(f"executeSQL-->{sql}")
        curs = connection.cursor()
        curs.execute(sql)

        if iter_status:
            return self.__search_iter(curs=curs, output_field=output_field, dict_status=dict_status, connection=connection)
        else:
            result = list()
            one_result = self.__search_iter(curs=curs, output_field=output_field, dict_status=dict_status)
            while True:
                try:
                    result.append(one_result.__next__())
                except:
                    curs.close()
                    connection.close()
                    return result

    def create_new_table_phoenix(self, sql):
        """
        创建新表
        :param sql: SQL 语句
        :return:
        """
        # pho_logger = Logger().logger
        self.cursor.execute(sql)
        self.conn.commit()
        pho_logger.info("{} 表创建成功".format(self.table_name))

    def create_table_phoenix(self, column_arrary_list, table_name=None):
        """
        创建表
        :param column_arrary_list: 要创建表的 列族名,列名 组成的列表 exp: [("C", "ID_"), ("C", "ENTITY_CODE_")]
        :param table_name: 要创建表的表名, 默认为创建 PhoenixHbase 对象时传入的表名
        :return: None
        """
        if not table_name:
            table_name = self.table_name

        column_sql_list = list()
        for each_column in column_arrary_list:
            if each_column[1] == "ID_":
                continue
            column_sql = f'"{each_column[0]}"."{each_column[1]}" VARCHAR'
            column_sql_list.append(column_sql)

        sql = f'CREATE TABLE "{table_name}" ("ID_" varchar primary key,{",".join(column_sql_list)})IMMUTABLE_ROWS = true'

        self.cursor.execute(sql)
        self.conn.commit()
        pho_logger.info("{} 表创建成功".format(self.table_name))

    def copy_table_phoenix(self, source_table, new_table, modify_dict=None, create_status=False):
        """
        复制数据到新的表中
        :param source_table: 数据来源的表名
        :param new_table: 数据去向的表名
        :param modify_dict: 需要修改的数据 type: dict
        :param create_status: 是否创建 new_table 表, 默认为否
        :return: None
        """
        if create_status:
            self.__get_coloumn(table_name=source_table)

            column_arrary_list = list()
            for i in range(1, len(self.position_dict)):
                if self.position_dict[i] == "CONTENT_":
                    column_arrary_list.append(("T", self.position_dict[i]))
                else:
                    column_arrary_list.append(("C", self.position_dict[i]))

            self.create_table_phoenix(column_arrary_list=column_arrary_list,
                                      table_name=new_table)

        result_generator = self.search_all_from_phoenix(dict_status=True)

        self.table_name = new_table

        while True:
            try:
                result = result_generator.__next__()
                if modify_dict:
                    result.update(modify_dict)
            except StopIteration:
                break

            self.upsert_to_phoenix_by_one(data=result)

    def drop_table_phoenix(self, table_name=None):
        """
        删除已存在的表
        :param table_name: 需删除的表名
        :return: None
        """
        # ALTER TABLE CHA_BRANCH_WECHAT DROP COLUMN IMAGE_
        # pho_logger = Logger().logger

        if not table_name:
            table_name = self.table_name
        self.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        pho_logger.info(f"表 {table_name} 删除成功")
        self.conn.commit()

    def add_column_phoenix(self, column_arrary_list, table_name=None):
        """
        为已存在的表添加新的列族和列
        :param column_arrary_list: 需添加的 列族名,列名 组成的列表 exp: [("C", "ID_"), ("C", "ENTITY_CODE_")]
        :param table_name: 需添加列的表名, 默认为创建 PhoenixHbase 对象时传入的表名
        :return:
        """
        # pho_logger = Logger().logger

        if not table_name:
            table_name = self.table_name

        column_sql_list = list()
        for each_column in column_arrary_list:
            column_sql = f'"{each_column[0]}"."{each_column[1]}" VARCHAR'
            column_sql_list.append(column_sql)

        sql = f'ALTER TABLE "{table_name}" ADD {",".join(column_sql_list)}'
        self.cursor.execute(sql)
        self.conn.commit()
        pho_logger.info(f"表 {table_name} 添加列 {str(column_arrary_list)[1:-1]} 成功")

    def delete_from_phoenix(self, connection=None, where_condition=None):
        """
        删除数据
        :param connection: phoenix 连接对象
        :param where_condition: where 条件
        :return:
        """
        # pho_logger = Logger().logger
        connection = PhoenixHbasePool.__pool.connection()
        sql = f"DELETE FROM {self.table_name}"

        if where_condition:
            if "where" in where_condition[:5] or "WHERE" in where_condition[:5]:
                where_condition = where_condition[5:]
            if where_condition[-2] == ",":
                where_condition = where_condition.replace(",", "")
            sql = sql + f" WHERE {where_condition}"
        curs = connection.cursor()
        curs.execute(sql)
        pho_logger.info(f"数据删除成功")
        connection.commit()
        curs.close()
        connection.close()

    def upsert_to_phoenix(self, data_list, connection=None):
        # pho_logger = Logger().logger
        connection = PhoenixHbasePool.__pool.connection()
        curs = connection.cursor()
        curs.setinputsizes(52428800)
        self.count = 0
        for data in data_list:
            batch_key = list()
            batch_value_ = list()
            batch_value = list()

            for key, value in data.items():
                value = str(value).replace("[", "(")
                value = value.replace("]", ")")
                value = value.replace("\'", "\"")

                batch_key.append(key)
                batch_value.append(value)

            batch_key_ = str(batch_key).replace("[", "(")
            batch_key_ = batch_key_.replace("]", ")")
            batch_key_ = batch_key_.replace("'", "")
            sql = 'upsert into \"{}\" {} values(?{})'.format(self.table_name, batch_key_, (",?" * (len(batch_key) - 1)))

            batch_value_.append(batch_value)

            curs.executemany(sql, batch_value_)
            connection.commit()

            self.count += 1
        curs.close()
        connection.close()

        pho_logger.info("插入成功{}条".format(self.count))

        return self.count

    def upsert_to_phoenix_by_one(self, data, table_name=None, connection=None):
        """
        插入单条数据
        :param connection: phoenix 连接对象
        :param data: 需要插入的数据
        :param table_name: 需要插入的表名
        :return:
        """
        connection = PhoenixHbasePool.__pool.connection()
        sql = None
        batch_key = list()
        batch_value_ = list()
        batch_value = list()

        for key, value in data.items():
            value = value_replace(value)
            batch_key.append(key)
            batch_value.append(value)

        batch_key_ = str(batch_key).replace("[", "(")
        batch_key_ = batch_key_.replace("]", ")")
        batch_key_ = batch_key_.replace("\'", "")
        if table_name:
            sql = 'upsert into \"{}\" {} values(?{})'.format(table_name, batch_key_, (",?" * (len(batch_key) - 1)))
        else:
            sql = 'upsert into \"{}\" {} values(?{})'.format(self.table_name, batch_key_, (",?" * (len(batch_key) - 1)))
        # print(sql)
        batch_value_.append(batch_value)
        # print(batch_value)
        curs = connection.cursor()
        curs.setinputsizes(52428800)
        curs.setoutputsize(52428800)

        curs.executemany(sql, batch_value_)
        connection.commit()

        self.count += 1
        curs.close()
        connection.close()

        return self.count

    def change_error_commonbidding(self):
        '''
        :return:
        '''
        self.table_name = "CommonBidding"
        result_generator = self.search_all_from_phoenix(dict_status=True, where_condition="ENTITY_STATUS_ = 'ERROR'")
        while True:
            try:
                result = result_generator.__next__()
                result["ENTITY_STATUS_"] = "DRAFT"
            except StopIteration:
                break

            self.upsert_to_phoenix_by_one(data=result)

    def phoenix_dispose(self):
        """
        @summary: 释放连接池资源, 不是真正的关闭,而是放回连接池
        """
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    # 更新hbase的 逻辑, 先查出 hbase 的数据 修改后, 在 upsert
    import pymongo
    import time
    import copy
    table_name = "CHA_BRANCH_NEWS"
    test1_ = PhoenixHbasePool(table_name=table_name)
    # connection = test1_.connect_to_phoenix()
    # client = pymongo.MongoClient(host="172.22.69.35", port=20000, serverSelectionTimeoutMS=60, connectTimeoutMS=60, connect=False)
    # db = client["spider_data_old"]
    cur = test1_.cursor
    time_array = time.localtime()
    create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    count = 0

    # 返回生成器对象
    # sql = ''' select count(1) as num from CHA_BRANCH_FUND_AGENCY '''
    # test1_.cursor.execute(sql)
    #
    # print(test1_.cursor.fetchall())
    # test1_.conn.close()
    result_generator = test1_.search_all_from_phoenix(dict_status=True, output_field=None, where_condition="""ID_ = 'ZX0000002001HYBG' or ID_ = 'ZX0000018235GWDT'""")

    while True:
        try:
            data = result_generator.__next__()
            # print(result_generator)
        except StopIteration:
            break
        else:
            try:
                print(data)
                # test1_.upsert_to_phoenix_by_one(data=data)
            except jaydebeapi.DatabaseError as e:
                print(2, e)
                continue
            except Exception as e:
                print(data.get('URL_'), e)
            count += 1
            print(count)
    test1_.conn.close()
