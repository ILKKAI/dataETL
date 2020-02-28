# -*- coding: utf-8 -*-
import csv
import os
import time
import arrow
import pymongo

from database._mongodb import MongoClient


class Statistics(object):
    def __init__(self, entity_type=None):
        """
        初始化
        :param entity_type:
        """
        t = arrow.now()
        self.local_time = t.format("YYYY-MM-DD")
        h_t = t.shift(days=-1)
        self.hesternal_time = h_t.format("YYYY-MM-DD")
        self.entity_type = entity_type
        self.__base_path = os.path.abspath(os.path.dirname(__file__))
        self.__dir_path = self.__base_path + "/scripts/{}".format(self.entity_type)
        self.file_path = self.__base_path + "/statistics/{}".format(self.local_time)
        self.__type_list = list()
        self.__file_list = list()
        if self.entity_type:
            self.get_entity_code()
        self.mongo_client = MongoClient()
        # "hesternal_spider_url_temp": 0, "hesternal_spider_url_fixed": 0,
        self.name_dict = {"实体编码": "", "待爬数据": 0, "需爬取总量": 0, "现有数据": 0, "昨日爬取数据": 0}

    def get_entity_code(self):
        """
        获取目标目录下文件名(去除 "CommonBidding_" 后就是 ENTITY_CODE_ )
        :return:
        """
        for root, dirs, files in os.walk(self.__dir_path):
            # print(root)  # 当前目录路径
            # print(dirs)  # 当前路径下所有子目录
            # print(files)  # 当前路径下所有非目录子文件
            self.__file_list = files
            self.__file_list.remove("__init_____.py")
            break

    def save_to_csv(self, file_path):
        """
        save each count and save to csv
        :param file_path:
        :return:
        """
        if os.path.exists(file_path):
            with open(file_path, "a", newline="", errors="ignore") as f:
                writer = csv.writer(f)
                append_list = list()
                for key, value in self.name_dict.items():
                    append_list.append(value)
                writer.writerows([append_list])
        else:
            try:
                with open(file_path, "a", newline="", errors="ignore") as f:
                    writer = csv.writer(f)
                    check_list = list()
                    append_list = list()
                    for key, value in self.name_dict.items():
                        check_list.append(key)
                        append_list.append(value)
                    writer.writerows([check_list])
                    writer.writerows([append_list])
            except FileNotFoundError:
                os.makedirs(self.file_path)
                with open(file_path, "a", newline="", errors="ignore") as f:
                    writer = csv.writer(f)
                    check_list = list()
                    append_list = list()
                    for key, value in self.name_dict.items():
                        check_list.append(key)
                        append_list.append(value)
                    writer.writerows([check_list])
                    writer.writerows([append_list])

    def count_from_database(self):
        """
        count data for database "spider_url_temp", "spider_url_fixed", "spider_data" where entity_code == entity_type
        :return:
        """
        # test_index = self.__file_list.index("CommonBidding_650500HMSSY.py")
        # self.__file_list = self.__file_list[test_index:]
        for entity_code in self.__file_list:
            entity_code = entity_code.replace("CommonBidding_", "")
            entity_code = entity_code.replace(".py", "")
            print(entity_code)
            self.name_dict["实体编码"] = entity_code

            # spider_url_temp
            db = self.mongo_client.client["spider_url_temp"]
            collection = db[entity_code]
            # 统计该实体所有数据
            try:
                mongo_data_list_temp = self.mongo_client.all_from_mongodb(collection=collection)
                if mongo_data_list_temp:
                    self.name_dict["待爬数据"] = mongo_data_list_temp.count()
                else:
                    self.name_dict["待爬数据"] = 0
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(5)
                mongo_data_list_temp = self.mongo_client.all_from_mongodb(collection=collection)
                if mongo_data_list_temp:
                    self.name_dict["待爬数据"] = mongo_data_list_temp.count()
                else:
                    self.name_dict["待爬数据"] = 0

            # 统计该实体昨天数据
            # try:
            #     temp_day_ago = collection.find({"$and": [{"DATETIME_": {"$gte": self.hesternal_time}},
            #                                              {"DATETIME_": {"$lte": self.local_time}}]})
            #     if temp_day_ago:
            #         self.name_dict["hesternal_spider_url_temp"] = temp_day_ago.count()
            #     else:
            #         self.name_dict["hesternal_spider_url_temp"] = 0
            # except pymongo.errors.ServerSelectionTimeoutError:
            #     time.sleep(5)
            #     temp_day_ago = collection.find({"$and": [{"DATETIME_": {"$gte": self.hesternal_time}},
            #                                              {"DATETIME_": {"$lte": self.local_time}}]})
            #     if temp_day_ago:
            #         self.name_dict["hesternal_spider_url_temp"] = temp_day_ago.count()
            #     else:
            #         self.name_dict["hesternal_spider_url_temp"] = 0

            # spider_url_fixed
            db = self.mongo_client.client["spider_url_fixed"]
            collection = db[entity_code]

            # 统计该实体所有数据
            try:
                mongo_data_list_fixed = self.mongo_client.all_from_mongodb(collection=collection)
                if mongo_data_list_fixed:
                    self.name_dict["需爬取总量"] = mongo_data_list_fixed.count()
                else:
                    self.name_dict["需爬取总量"] = 0
            except pymongo.errors.ServerSelectionTimeoutError:
                time.sleep(5)
                mongo_data_list_fixed = self.mongo_client.all_from_mongodb(collection=collection)
                if mongo_data_list_fixed:
                    self.name_dict["需爬取总量"] = mongo_data_list_fixed.count()
                else:
                    self.name_dict["需爬取总量"] = 0

            # # 统计该实体昨天数据
            # try:
            #     fixed_day_ago = collection.find({"$and": [{"DATETIME_": {"$gte": self.hesternal_time}},
            #                                               {"DATETIME_": {"$lte": self.local_time}}]})
            #     if fixed_day_ago:
            #         self.name_dict["hesternal_spider_url_fixed"] = fixed_day_ago.count()
            #     else:
            #         self.name_dict["hesternal_spider_url_fixed"] = 0
            # except pymongo.errors.ServerSelectionTimeoutError:
            #     time.sleep(5)
            #     fixed_day_ago = collection.find({"$and": [{"DATETIME_": {"$gte": self.hesternal_time}},
            #                                               {"DATETIME_": {"$lte": self.local_time}}]})
            #     if fixed_day_ago:
            #         self.name_dict["hesternal_spider_url_fixed"] = fixed_day_ago.count()
            #     else:
            #         self.name_dict["hesternal_spider_url_fixed"] = 0

            # spider_data
            db = self.mongo_client.client["spider_data"]
            collection = db[self.entity_type]
            self.mongo_client.mongo_entity_code = entity_code
            if len(self.__file_list) == 1:
                # 统计该实体所有数据
                try:
                    mongo_data_list_data = self.mongo_client.all_from_mongodb(collection=collection)
                    if mongo_data_list_data:
                        self.name_dict["现有数据"] = mongo_data_list_data.count()
                    else:
                        self.name_dict["现有数据"] = 0
                except pymongo.errors.ServerSelectionTimeoutError:
                    time.sleep(5)
                    mongo_data_list_data = self.mongo_client.all_from_mongodb(collection=collection)
                    if mongo_data_list_data:
                        self.name_dict["现有数据"] = mongo_data_list_data.count()
                    else:
                        self.name_dict["现有数据"] = 0
                # 统计该实体昨天数据
                try:
                    data_day_ago = collection.find({"$and": [{"DATETIME_": {"$gte": self.hesternal_time}},
                                                             {"DATETIME_": {"$lte": self.local_time}}]})
                    if data_day_ago:
                        self.name_dict["昨日爬取数据"] = data_day_ago.count()
                    else:
                        self.name_dict["昨日爬取数据"] = 0
                except pymongo.errors.ServerSelectionTimeoutError:
                    time.sleep(5)
                    data_day_ago = collection.find({"$and": [{"DATETIME_": {"$gte": self.hesternal_time}},
                                                             {"DATETIME_": {"$lte": self.local_time}}]})
                    if data_day_ago:
                        self.name_dict["昨日爬取数据"] = data_day_ago.count()
                    else:
                        self.name_dict["昨日爬取数据"] = 0
            else:
                # 统计该实体所有数据
                try:
                    mongo_data_list_data = self.mongo_client.search_from_mongodb(collection=collection)
                    if mongo_data_list_data:
                        self.name_dict["现有数据"] = mongo_data_list_data.count()
                    else:
                        self.name_dict["现有数据"] = 0
                except pymongo.errors.ServerSelectionTimeoutError:
                    time.sleep(5)
                    mongo_data_list_data = self.mongo_client.search_from_mongodb(collection=collection)
                    if mongo_data_list_data:
                        self.name_dict["现有数据"] = mongo_data_list_data.count()
                    else:
                        self.name_dict["现有数据"] = 0
                # 统计该实体昨天数据
                try:
                    data_day_ago = collection.find({"$and": [{"ENTITY_CODE_": entity_code},
                                                             {"DATETIME_": {"$gte": self.hesternal_time}},
                                                             {"DATETIME_": {"$lte": self.local_time}}]})
                    if data_day_ago:
                        self.name_dict["昨日爬取数据"] = data_day_ago.count()
                    else:
                        self.name_dict["昨日爬取数据"] = 0
                except pymongo.errors.ServerSelectionTimeoutError:
                    time.sleep(5)
                    data_day_ago = collection.find({"$and": [{"ENTITY_CODE_": entity_code},
                                                             {"DATETIME_": {"$gte": self.hesternal_time}},
                                                             {"DATETIME_": {"$lte": self.local_time}}]})
                    if data_day_ago:
                        self.name_dict["昨日爬取数据"] = data_day_ago.count()
                    else:
                        self.name_dict["昨日爬取数据"] = 0

            file_path = self.file_path + "/count_for_{}.csv".format(self.entity_type)
            self.save_to_csv(file_path)
        self.mongo_client.client_close()

    def run(self):
        if self.entity_type:
            self.count_from_database()
        else:
            for root, dirs, files in os.walk(self.__base_path + "/scripts"):
                # print(root)  # 当前目录路径
                # print(dirs)  # 当前路径下所有子目录
                # print(files)  # 当前路径下所有非目录子文件
                self.__type_list = dirs
                self.__type_list.remove("__pycache__")
                break
            # 中断
            # test_index = self.__type_list.index("NEWS_FINASSIST")
            # self.__type_list = self.__type_list[test_index:]
            for _type in self.__type_list:
                print(_type)
                self.entity_type = _type
                self.__dir_path = self.__base_path + "/scripts/{}".format(_type)
                self.get_entity_code()
                self.count_from_database()


if __name__ == '__main__':
    script = Statistics()
    # script = Statistics("CommonBidding")
    script.run()
