# -*- coding: utf-8 -*-
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
import base64
import threading
import queue
import random
import time
from dev_plan_scripts.mysql_db import Config, MyPymysqlPool


class Generate_task(threading.Thread):

    def __init__(self, store, queue, values=()):
        '''
        消费者
        FIFO即 First in First Out,先进先出。Queue提供了一个基本的FIFO容器，使用方法很简单,maxsize是个整数，指明了队列中能存放的数据个数的上限。一旦达到上限，插入会导致阻塞，直到队列中的数据被消费掉。如果maxsize小于或者等于0，队列大小没有限制
        可自定义任务函数,
        :param store:
        :param queue: 队列对象
        :param values: 用于切片的值 int
        '''
        threading.Thread.__init__(self)
        self.queue = queue
        self.store = store
        self.data = []
        self.values = values
        self.result = ''


        collection_name = ''
        # MongoDB connection
        self.m_client = MongoClient(mongo_collection=collection_name)
        self.db, self.collection_list = self.m_client.client_to_mongodb()
        # MongoDB old connection
        # spider_data_old 的表连接是遍历查询出来的,,所以需要手动建立
        self.old_client = MongoClient(mongo_collection=collection_name)
        self.old_client.mongo_db = "spider_data_old"

    def rask(self, *args, **kwargs):
        pass

    def get_result(self, result):
        '''
        线程你不好追踪返回值, 所有座位属性返回
        :param result:
        :return:
        '''
        self.result = result

    def run(self):
        try:
            self.queue.get()  # 获取任务
            # 写入函数
            print('This is store %s' % self.store)
            result = self.rask()
            self.get_result(result)
        except Exception as e:
            print(e)
        finally:
            self.queue.task_done()  # 通知任务结束


class Executer_task(Generate_task):
    '''
    动态创建任务
    :param store:
    :param queue: 队列对象
    :param values: 用于切片的值 int
    '''

    def rask(self, ):
        data = self.data[self.values:self.values+1]
        time.sleep(3)
        return random.randint(1,5)


class Executer_development_plan(Generate_task):

    def run(self):
        pass


if __name__ == '__main__':
    print()
    replace_dict = {'x.docx': '.docx', '.PDF.pdf': '.pdf', '．doc.doc': '.doc', '..xls': '.xls'}
    path = os.path.dirname(__file__) + '/Files/'
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            for key, value in replace_dict.items():
                if key in name:
                    os.rename(path + name, path + name.replace(key, value))

