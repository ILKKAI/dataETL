# -*- coding: utf-8 -*-
import traceback

from __config import *
from log.data_log import Logger
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import pymysql, os
import threading
from functools import wraps


def optional_debug(func):
    @wraps(func)
    def wrapper(*args, conn=None, **kwargs):
        connection = conn.connection()
        func(*args, connection=connection, **kwargs)
        connection.close()
    return wrapper


class BasePymysqlPool(object):
    def __init__(self, entity_code=None, **kwargs):
        if not kwargs:
            self.mysql_host = MYSQL_HOST
            self.mysql_port = MYSQL_PORT
            self.mysql_database = MYSQL_DATABASE
            self.mysql_table = MYSQL_TABLE
            self.mysql_user = MYSQL_USER
            self.mysql_password = MYSQL_PASSWORD

        else:
            self.mysql_host = kwargs["host"]
            self.mysql_port = kwargs["port"]
            self.mysql_database = kwargs["database"]
            self.mysql_table = kwargs["table"]
            self.mysql_user = kwargs["user"]
            self.mysql_password = kwargs["password"]

        self.mysql_entity_code = entity_code

        self.mysql_config = {
            "host": self.mysql_host,
            "port": self.mysql_port,
            "database": self.mysql_database,
            "user": self.mysql_user,
            "password": self.mysql_password,
            "charset": "utf8"
        }


class MyPymysqlPool(BasePymysqlPool):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
            释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None
    _instance_lock = threading.Lock()

    def __init__(self, entity_code=None, **kwargs):
        super(MyPymysqlPool, self).__init__(**kwargs)  # 传入父类的实例化参数
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = self.__getConn()

    # def __new__(cls, *arg, **kwargs):
    #     '''
    #     单例模式的实现思路, 在 __new__ 动态的给类设置一个属性, 在每次实例化新的实例时判断属性是否存在,若存在直接返回单例, 不存在创建单例再返回
    #     :param arg:
    #     :param kwargs:
    #     :return:
    #     '''
    #     if not hasattr(MyPymysqlPool, '_instance'):
    #         with MyPymysqlPool._instance_lock:
    #             if not hasattr(MyPymysqlPool, '_instance'):
    #                 MyPymysqlPool._instance = object.__new__(cls)
    #     return MyPymysqlPool._instance

    def __getConn(self):
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if MyPymysqlPool.__pool is None:
            # creator 传入 phoenix 的产生连接池的函数
            __pool = PooledDB(creator=pymysql,
                              mincached=1,
                              maxcached=1000,
                              host=self.mysql_host,
                              port=self.mysql_port,
                              user=self.mysql_user,
                              passwd=self.mysql_password,
                              database=self.mysql_database,
                              use_unicode=True,
                              charset="utf8",  # 指定字符编码
                              autocommit=True,
                              cursorclass=pymysql.cursors.DictCursor,)
            self.__pool = __pool
        return __pool.connection()

    # 不获取返回值的查询
    def query(self, sql, param=None):
        connection = self.__pool.connection()
        _cursor = connection.cursor()
        if param is None:
            count = _cursor.execute(sql)
        else:
            count = _cursor.execute(sql, param)
        _cursor.close()
        connection.close()
        return count

    # 有返回值得查询
    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        connection = self.__pool.connection()
        _cursor = connection.cursor()
        if param is None:
            count = _cursor.execute(sql)
        else:
            count = _cursor.execute(sql, param)
        if count > 0:
            result = _cursor.fetchall()
        else:
            result = False
        _cursor.close()
        connection.close()
        return result  #

    # 有返回值得查询
    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        connection = self.__pool.connection()
        _cursor = connection.cursor()
        if param is None:
            count = _cursor.execute(sql)
        else:
            count = _cursor.execute(sql, param)
        if count > 0:
            result = _cursor.fetchone()
        else:
            result = False
        _cursor.close()
        connection.close()
        return result

    # 有返回值得查询
    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        connection = self.__pool.connection()
        _cursor = connection.cursor()
        if param is None:
            count = _cursor.execute(sql)
        else:
            count = _cursor.execute(sql, param)
        if count > 0:
            result = _cursor.fetchmany(num)
        else:
            result = False
        _cursor.close()
        connection.close()
        return result

    # 批量插入
    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        connection = self.__pool.connection()
        _cursor = connection.cursor()
        count = _cursor.executemany(sql, values)
        _cursor.close()
        connection.close()
        return count

    # 单条数据插入
    def insert(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    # 更新数据表记录
    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    # def to_connect(self):
    #     if self.__pool:
    #         return self.__pool.connection()
    #
    # def is_connected(self):
    #     """Check if the server is alive"""
    #     try:
    #         self._conn.ping(reconnect=True)
    #     except:
    #         traceback.print_exc()
    #         try:
    #             self._conn = self.to_connect()
    #             self._cursor = self._conn.cursor()
    #         except:
    #             pass


count = 0
t = MyPymysqlPool()
def print_data():
    '''
    单独获取连接, 产生游标和管边游标和连接
    :return:
    '''
    # t = MyPymysqlPool()
    global count
    # t.is_connected()
    print(t.getAll('''select count(*) from cha_di_position'''))
    count += 1
    print(f'我被执行, 编号{count}')
    # t.dispose()


if __name__ == '__main__':

    # print(config.get_options(config.get_sections()))
    # print(config.get_content())  spi_auto_login
    import time
    import threading
    for _ in range(100):
        data = threading.Thread(target=print_data, args=())
        # time.sleep(2)
        data.start()
