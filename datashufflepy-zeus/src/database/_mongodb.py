# -*- coding: utf-8 -*-
"""MongoDB 连接"""
import pymongo
from bson.objectid import ObjectId
from __config import *
from log.data_log import Logger


class BaseClient(object):

    def __init__(self, entity_code=None, **kwargs):

        if not kwargs:
            # self.mongo_host = "172.22.69.35"
            self.mongo_host = "localhost"
            # self.mongo_port = 20000
            self.mongo_port = 27017
            self.mongo_database = "spider_data"
        else:
            try:
                self.mongo_host = kwargs["host"]
            except KeyError:
                self.mongo_host = MONGO_HOST_35
            try:
                self.mongo_port = kwargs["port"]
            except KeyError:
                # self.mongo_port = MONGO_PORT_20
                self.mongo_port = 27017
            try:
                self.mongo_database = kwargs["database"]
            except KeyError:
                self.mongo_database = MONGO_DB
        self.client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port, serverSelectionTimeoutMS=60,
                                          connectTimeoutMS=60, connect=False)
        self.db = self.client[self.mongo_database]
        self.mongo_entity_code = entity_code


class TMongoClient(BaseClient):
    def __init__(self, entity_code=None, collection=None, mongo_config=None):
        if mongo_config:
            self.mongo_config = mongo_config
        else:
            self.mongo_config = {}

        self.mongo_collection = collection if collection else None

        super(TMongoClient, self).__init__(entity_code=entity_code, **self.mongo_config)

    def client_to_mongodb(self):
        mon_logger = Logger().logger
        mon_logger.info("开始连接MongoDB({}:{}),database={}".format(self.mongo_host, self.mongo_port, self.mongo_database))
        try:
            collection_list = self.db.collection_names()
            mon_logger.info("MongoDB({}:{})连接成功".format(self.mongo_host, self.mongo_port))
            return collection_list
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.warning("MongoDB({}:{})连接失败".format(self.mongo_host, self.mongo_port))
            for i in range(2, 6):
                try:
                    collection_list = self.db.collection_names()
                    mon_logger.info("MongoDB({}:{})连接成功".format(self.mongo_host, self.mongo_port))
                    return collection_list
                except Exception:
                    mon_logger.warning("MongoDB({}:{})第{}次连接失败".format(self.mongo_host, self.mongo_port, i))
                if i == 5:
                    mon_logger.error(
                        "MongoDB连接失败，错误信息为: {}, 请检查各项参数是否正确host={}, port={},database={}".format(e,
                                                                                                self.mongo_host,
                                                                                                self.mongo_port,
                                                                                                self.mongo_database))
                    self.client_close()

    def get_check_collection(self, collection_list):
        mon_logger = Logger().logger
        if self.mongo_collection in collection_list:
            collection = self.db[self.mongo_collection]
            return collection
        else:
            mon_logger.error("MongoDB没有该集合，请检查")
            self.client_close()
            # raise Exception("MongoDB没有该集合，请检查")

    def get_data_from_mongodb(self, collection, entity_code=None, exclude_code=None, limit_number=None, data_id=None, find_query=None):
        """
        从 MongoDB 获取数据
        :param collection:
        :param entity_code: 需要查取的 实体编码
        :param exclude_code: 需要排除的 实体编码
        :param limit_number: 查取的数据条数
        :param data_id: 查取 data_id 后的数据
        :return:
        """
        mon_logger = Logger().logger
        query_list = list()
        if isinstance(find_query, dict):
            query_list.append(find_query)
        elif isinstance(find_query, list):
            query_list.extend(find_query)
        if isinstance(entity_code, str):
            query_list.append({"ENTITY_CODE_": entity_code})
        elif isinstance(entity_code, (list, tuple)):
            query_list.append({"ENTITY_CODE_": {"$in": list(entity_code)}})

        if isinstance(exclude_code, str):
            query_list.append({"ENTITY_CODE_": {"$ne": exclude_code}})
        elif isinstance(exclude_code, (list, tuple)):
            query_list.append({"ENTITY_CODE_": {"$nin": list(exclude_code)}})

        if data_id:
            find_id = ObjectId(data_id)
            query_list.append({"_id": {"$gte": find_id}})

        if query_list:
            query = {"$and": query_list}
        else:
            query = {}
        try:
            mon_logger.info("MongoDB 开始查取数据")
            result_one = collection.find_one(query)
            if result_one:
                if limit_number:
                    result = collection.find(query, no_cursor_timeout=True).limit(int(limit_number))
                    if entity_code:
                        mon_logger.info(f"ENTITY: {entity_code} 数据查取成功共 {limit_number} 条")
                    else:
                        mon_logger.info("数据查取成功共 {}条".format(limit_number))
                else:
                    result = collection.find(query, no_cursor_timeout=True)
                    if entity_code:
                        mon_logger.info(f"ENTITY: {entity_code} 数据查取成功共 {result.count()}条")
                    else:
                        mon_logger.info("数据查取成功共 {}条".format(result.count()))

                return result
            else:
                if entity_code:
                    mon_logger.info("ENTITY: {} 数据查取为空".format(self.mongo_entity_code))
                else:
                    mon_logger.info("数据查取为空".format(self.mongo_entity_code))
                return None
        except TypeError as e:
                mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 {}".format(e, self.mongo_entity_code))
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
            result_one = collection.find_one(query)
            if result_one:
                if limit_number:
                    result = collection.find(query, no_cursor_timeout=True).limit(int(limit_number))
                    if entity_code:
                        mon_logger.info(f"ENTITY: {entity_code} 数据查取成功共 {limit_number} 条")
                    else:
                        mon_logger.info("数据查取成功共 {}条".format(limit_number))
                else:
                    result = collection.find(query, no_cursor_timeout=True)
                    if entity_code:
                        mon_logger.info(f"ENTITY: {entity_code} 数据查取成功共 {result.count()}条")
                    else:
                        mon_logger.info("数据查取成功共 {}条".format(result.count()))

                return result
            else:
                if entity_code:
                    mon_logger.info("ENTITY: {} 数据查取为空".format(self.mongo_entity_code))
                else:
                    mon_logger.info("数据查取为空".format(self.mongo_entity_code))
                return None

    # todo only can return one output
    def match_from_mongo(self, collection, match, output):
        """
        查询所有数据, 返回游标对象(聚合)
        :param collection:
        :param match: match condition like dict {"ENTITY_CODE_": "XXXXXXXXX"}
        :param output: output field like list or dict
        :return:
        """
        mon_logger = Logger().logger
        if isinstance(output, str):
            output = [output]
        try:
            mon_logger.info("MongoDB 开始查取数据")
            output_condition = dict()
            for o in output:
                output_condition[o] = 1
            result = collection.aggregate([{"$match": match}, {"$project": output_condition}])
            mon_logger.info("MongoDB 数据查取成功")
            return result

        except TypeError as e:
            mon_logger.error("WEIBO_CODE_ 数据查取失败,错误信息为{}, 请检查匹配规则是否正确:{}".format(e, match))
            # raise Exception("WEIBO_CODE_ 查取失败, 错误信息为{}".format(e))

        finally:
            self.client_close()

    def all_to_mongodb(self, collection, insert_list):
        """
        插入数据
        :param collection:
        :param insert_list: list
        :return:
        """
        mon_logger = Logger().logger
        try:
            result = collection.insert_many(insert_list)
            mon_logger.info("MongoDB 数据插入成功, 成功条数 {} 条".format(len(result.inserted_ids)))
            return len(result.inserted_ids)

        except TypeError as e:
            mon_logger.info("MongoDB 数据插入失败,错误信息为 {}".format(e))
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
            result = collection.insert_many(insert_list)
            mon_logger.info("数据插入成功, 成功条数 {} 条".format(len(result.inserted_ids)))
            return len(result.inserted_ids)
        except Exception as e:
            mon_logger.info("MongoDB 数据插入失败,错误信息为 {}".format(e))
            return 0
        finally:
            self.client_close()

    def remove_from_mongo(self, collection, remove_id_list, delete_condition=None):
        """
        根据 _id 删除数据
        :param collection:
        :param remove_id_list:
        :param delete_condition:  dict like {"ENTITY_CODE_": "XXXXXXXX"}
        :return:
        """
        if isinstance(remove_id_list, str):
            remove_id_list = [remove_id_list]
        # mon_logger = Logger().logger
        if delete_condition:
            result = collection.delete_many({"$and": [
                delete_condition,
                {"_id": {"$in": remove_id_list}}
            ]})
        else:
            result = collection.delete_many({"_id": {"$in": remove_id_list}})

        # mon_logger.info("ENTITY_CODE_: {} 删除成功 {}条".format(self.mongo_entity_code, result.deleted_count))
        return result.deleted_count

    # todo updata_one str is not done
    def update_to_mongodb(self, collection, data_id, data_dict):
        """
        根据 _id 更新数据
        :param collection:
        :param data_id: str or list
        :param data_dict: dict
        :return:
        """
        # mon_logger = Logger().logger
        # try:
        if isinstance(data_id, list):
            result = collection.update_many({"_id": {"$in": data_id}}, {"$set": data_dict})
            # mon_logger.info("ENTITY_CODE_: {} 更新成功 {}条".format(self.mongo_entity_code, result.matched_count))
        elif isinstance(data_id, str):
            result = collection.update_one({"_id": data_id}, {"$set": data_dict})
        else:
            raise Exception("No support type of \"data_id\"")
        # mon_logger.info("ENTITY_CODE_: {} 更新成功 {}条".format(self.mongo_entity_code, result.matched_count))
        return result.matched_count

        # except Exception as e:
        #     mon_logger.info("ENTITY_CODE_: {} 更新失败: {}".format(self.mongo_entity_code, e))
        #     return None

    def client_close(self):
        """
        关闭连接
        :return:
        """
        self.client.close()


class MongoClient(object):

    def __init__(self, entity_code=None, mongo_collection=None):
        # self.mongo_list = MONGO_PRO
        # self.mongo_list = MONGO_HOST_35
        # self.mongo_list = MONGO_PORT_27
        self.mongo_list = MONGO_HOST_LH
        self.mongo_db = MONGO_DB
        self.mongo_collection = mongo_collection
        self.mongo_entity_code = entity_code

        self.client = pymongo.MongoClient(self.mongo_list, serverSelectionTimeoutMS=60,
                                          connectTimeoutMS=60, connect=False)

    def client_to_mongodb(self):
        mon_logger = Logger().logger
        mon_logger.info("开始连接MongoDB({}),database={}".format(self.mongo_list, self.mongo_db))

        db = self.client[self.mongo_db]
        try:
            collection_list = db.collection_names()
            mon_logger.info("MongoDB({})连接成功".format(self.mongo_list))
            return db, collection_list
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.warning("MongoDB({})连接失败".format(self.mongo_list))
            for i in range(2, 6):
                try:
                    collection_list = db.collection_names()
                    mon_logger.info("MongoDB({})连接成功".format(self.mongo_list))
                    return db, collection_list
                except Exception:
                    mon_logger.warning("MongoDB({})第{}次连接失败".format(self.mongo_list, i))
                if i == 5:
                    mon_logger.error(
                        "MongoDB连接失败，错误信息为: {}, 请检查各项参数是否正确{},database={}".format(e, self.mongo_list,
                                                                                                self.mongo_db))
                    self.client_close()

    def get_check_collection(self, db, collection_list):
        mon_logger = Logger().logger
        if self.mongo_collection in collection_list:
            collection = db[self.mongo_collection]
            return collection
        else:
            mon_logger.error(f"MongoDB {self.mongo_db} 没有 {self.mongo_collection} 集合，请检查")
            return
            # self.client_close()
            # raise Exception("MongoDB没有该集合，请检查")

    def get_data_and_update(self, collection, entity_code, exclude_code, update_dict, data_id=None, other_query=None,sort_query=None):
        """
        查询一条数据并更新
        :param collection:
        :param entity_code: 需要查取的 实体编码
        :param exclude_code: 需要排除的 实体编码
        :param update_dict: 需要更新的字段与值
        :param data_id: 查取 data_id 后的数据
        :param other_query: 其他过滤条件
        :return:
        """
        mon_logger = Logger().logger
        query_list = list()
        if isinstance(entity_code, str):
            query_list.append({"ENTITY_CODE_": entity_code})
        elif isinstance(entity_code, (list, tuple)):
            query_list.append({"ENTITY_CODE_": {"$in": list(entity_code)}})

        if isinstance(exclude_code, str):
            query_list.append({"ENTITY_CODE_": {"$ne": exclude_code}})
        elif isinstance(exclude_code, (list, tuple)):
            query_list.append({"ENTITY_CODE_": {"$nin": list(exclude_code)}})

        if isinstance(other_query, dict):
            query_list.append(other_query)
        elif isinstance(other_query, list):
            query_list.extend(other_query)

        if data_id:
            find_id = ObjectId(data_id)
            query_list.append({"_id": {"$gte": find_id}})

        if query_list:
            query = {"$and": query_list}
        else:
            query = {}
        try:
            mon_logger.info(f"query={query}")
            result_one = collection.find_one_and_update(query, update_dict, sort=sort_query)
            if result_one:
                mon_logger.info(f"MongoDB--数据查取并更新成功")
                return result_one
            else:
                mon_logger.info("MongoDB 数据查取为空".format(self.mongo_entity_code))
                return None
        except TypeError as e:
            mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 {}".format(e, self.mongo_entity_code))
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
            result_one = collection.find_one_and_update(query, update_dict)
            if result_one:
                mon_logger.info("MongoDB--数据查取并更新成功")
                return result_one
            else:
                mon_logger.info("MongoDB 数据查取为空".format(self.mongo_entity_code))
                return None

    def get_data_from_mongodb(self, collection, entity_code, exclude_code, limit_number, data_id=None, other_query=None):
        """
        从 MongoDB 获取数据
        :param collection:
        :param entity_code: 需要查取的 实体编码
        :param exclude_code: 需要排除的 实体编码
        :param limit_number: 查取的数据条数
        :param data_id: 查取 data_id 后的数据
        :param other_query: 其他过滤条件
        :return:
        """
        mon_logger = Logger().logger
        query_list = list()
        if isinstance(entity_code, str):
            query_list.append({"ENTITY_CODE_": entity_code})
        elif isinstance(entity_code, (list, tuple)):
            query_list.append({"ENTITY_CODE_": {"$in": list(entity_code)}})

        if isinstance(exclude_code, str):
            query_list.append({"ENTITY_CODE_": {"$ne": exclude_code}})
        elif isinstance(exclude_code, (list, tuple)):
            query_list.append({"ENTITY_CODE_": {"$nin": list(exclude_code)}})

        if isinstance(other_query, dict):
            query_list.append(other_query)
        elif isinstance(other_query, list):
            query_list.extend(other_query)

        if data_id:
            find_id = ObjectId(data_id)
            query_list.append({"_id": {"$gte": find_id}})

        if query_list:
            query = {"$and": query_list}
        else:
            query = {}
        try:
            mon_logger.info("MongoDB 开始查取数据")
            result_one = collection.find_one(query)
            if result_one:
                if limit_number:
                    result = collection.find(query, no_cursor_timeout=True).limit(int(limit_number))
                else:
                    result = collection.find(query, no_cursor_timeout=True)
                if result.count() > int(limit_number if limit_number else 999999999):
                    mon_logger.info("数据查取成功共 {}条".format(limit_number))
                else:
                    mon_logger.info("数据查取成功共 {}条".format(result.count()))
                return result
            else:
                # mon_logger.info("ENTITY: {} 数据查取为空".format(self.mongo_entity_code))
                mon_logger.info("数据查取为空".format(self.mongo_entity_code))
                return None
        except TypeError as e:
                mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 {}".format(e, self.mongo_entity_code))
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
            result_one = collection.find_one(query)
            if result_one:
                if limit_number:
                    result = collection.find(query, no_cursor_timeout=True).limit(int(limit_number))
                else:
                    result = collection.find(query, no_cursor_timeout=True)
                if result.count() > int(limit_number):
                    mon_logger.info("数据查取成功共 {}条".format(limit_number))
                else:
                    mon_logger.info("数据查取成功共 {}条".format(result.count()))
                return result
            else:
                mon_logger.info("MongoDB 查取数据为空")
                return None

    # 根据ENTITY_CODE_查询, 返回游标对象
    def search_from_mongodb(self, collection, field_name="ENTITY_CODE_", field_value={"$exists": True}, data_id=None):
        mon_logger = Logger().logger
        try:
            mon_logger.info("开始查取数据")
            if data_id:
                find_id = ObjectId(data_id)
                result_one = collection.find_one({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code}, {"_id": {"$gte": find_id}}, {field_name: field_value}]})
            else:
                result_one = collection.find_one({"$and": [{"ENTITY_CODE_": self.mongo_entity_code}, {field_name: field_value}]})
            if result_one is not None:
                result = collection.find({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code}, {"_id": {"$gte": result_one["_id"]}}, {field_name: field_value}]},
                    no_cursor_timeout=True)

                mon_logger.info("ENTITY: {} 数据查取成功共 {}条".format(self.mongo_entity_code, result.count()))
                return result
            else:
                mon_logger.info("ENTITY: {} 数据查取为空".format(self.mongo_entity_code))
                return None
        except TypeError as e:
            mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 ENTITY_CODE_ 是否正确:{}".format(e, self.mongo_entity_code))
        finally:
            self.client_close()

    # 根据ENTITY_CODE_查询, 返回游标对象
    def search_by_status(self, collection, data_id=None):
        mon_logger = Logger().logger
        try:
            mon_logger.info("开始查取数据")
            if data_id:
                find_id = ObjectId(data_id)
                result_one = collection.find_one({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code},
                    {"_id": {"$gte": find_id}},
                    {"d": {"$exists": False}}
                ]})
            else:
                result_one = collection.find_one({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code},
                    {"d": {"$exists": False}}
                ]})
            if result_one is not None:
                result = collection.find({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code},
                    {"_id": {"$gte": result_one["_id"]}},
                    {"d": {"$exists": False}}
                ]}, no_cursor_timeout=True)

                mon_logger.info("ENTITY: {} 数据查取成功共 {}条".format(result.count()))
                return result
            else:
                mon_logger.info("ENTITY: {} 数据查取为空".format(self.mongo_entity_code))
                return None
        except TypeError as e:
            mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 ENTITY_CODE_ 是否正确:{}".format(e, self.mongo_entity_code))
        finally:
            self.client_close()

    # 根据 TITLE_ 查询, 返回游标对象
    def search_title_from_mongodb(self, collection, data_id=None):
        mon_logger = Logger().logger
        try:
            mon_logger.info("开始查取数据")
            if data_id:
                find_id = ObjectId(data_id)
                result_one = collection.find_one({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code},
                    {"$or": [{"TITLE_": {"$exists": True}}, {"title": {"$exists": True}}]},
                    {"_id": {"$gte": find_id}}
                ]})
            else:
                result_one = collection.find_one({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code},
                    {"$or": [{"TITLE_": {"$exists": True}}, {"title": {"$exists": True}}]}
                ]})

            if result_one is not None:
                result = collection.find({"$and": [
                    {"ENTITY_CODE_": self.mongo_entity_code},
                    {"$or": [{"TITLE_": {"$exists": True}}, {"title": {"$exists": True}}]},
                    {"_id": {"$gte": result_one["_id"]}}
                ]}, no_cursor_timeout=True)

                mon_logger.info("ENTITY: {} 数据查取成功共 {}条".format(self.mongo_entity_code, result.count()))
                return result
            else:
                mon_logger.info("MongoDB 查取数据为空,请检查 ENTITY_CODE_ 是否正确:{}".format(self.mongo_entity_code))
                return None

        except TypeError as e:
            mon_logger.warning("MongoDB数据查取失败,错误信息为{}, 请检查 ENTITY_CODE_ 是否正确:{}".format(e, self.mongo_entity_code))

        finally:
            self.client_close()

    # 查询所有数据, 返回游标对象(聚合)
    def match_from_mongo(self, collection, match, output):
        mon_logger = Logger().logger
        try:
            mon_logger.info("开始查取数据")
            result = collection.aggregate([{"$match": match}, {"$project": {"budgetPrice": 1, "_id": 0, output: 1}}])
            for i in result:
                if i is not None:
                    mon_logger.info("数据查取成功")
                    return i[output]
                else:
                    mon_logger.error("WEIBO_CODE_ 查取数据为空")
                    # raise Exception("WEIBO_CODE_ 查取失败")

        except TypeError as e:
            mon_logger.error("WEIBO_CODE_ 数据查取失败,错误信息为{}, 请检查匹配规则是否正确:{}".format(e, match))
            raise Exception("WEIBO_CODE_ 查取失败, 错误信息为{}".format(e))

        finally:
            self.client_close()

    # 查询所有数据, 返回游标对象
    def all_from_mongodb(self, collection, data_id=None, d=False):
        mon_logger = Logger().logger
        if data_id:
            find_id = ObjectId(data_id)
            try:
                mon_logger.info("开始查取数据")
                # result = collection.find_one({"_id": {"$gte": find_id}})
                result = collection.find_one({"$and": [{"_id": {"$gte": find_id}}, {"ORDER_ID": {"$exists": False}}]})
                if result is not None:
                    result = collection.find({"$and": [{"_id": {"$gte": find_id}}, {"ORDER_ID": {"$exists": False}}]},
                                             no_cursor_timeout=True)
                    mon_logger.info("数据查取成功, 共 {} 条".format(result.count()))
                    return result
                else:
                    mon_logger.info("MongoDB 查取数据为空")
                    return None
            except TypeError as e:
                mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 {}".format(e, self.mongo_entity_code))
            except pymongo.errors.ServerSelectionTimeoutError as e:
                mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
                result = collection.find_one({"$and": [{"_id": {"$gte": find_id}}, {"ORDER_ID": {"$exists": False}}]})
                if result:
                    result = collection.find({"$and": [{"_id": {"$gte": find_id}}, {"ORDER_ID": {"$exists": False}}]},
                                             no_cursor_timeout=True)
                    mon_logger.info("数据查取成功, 共 {} 条".format(result.count()))
                    return result
                else:
                    mon_logger.info("MongoDB 查取数据为空")
                    return None
        else:
            if d:
                try:
                    mon_logger.info("开始查取数据")
                    result = collection.find_one()
                    if result is not None:
                        result = collection.find(no_cursor_timeout=True)
                        mon_logger.info("数据查取成功, 共 {} 条".format(result.count()))
                        return result
                    else:
                        mon_logger.info("MongoDB 查取数据为空")
                        return None
                except TypeError as e:
                    mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 {}".format(e, self.mongo_entity_code))
                except pymongo.errors.ServerSelectionTimeoutError as e:
                    mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
                    result = collection.find_one()
                    if result:
                        result = collection.find(no_cursor_timeout=True)
                        mon_logger.info("数据查取成功, 共 {} 条".format(result.count()))
                        return result
                    else:
                        mon_logger.info("MongoDB 查取数据为空")
                        return None
            else:
                try:
                    mon_logger.info("开始查取数据")
                    result = collection.find_one({"d": {"$exists": False}})
                    if result is not None:
                        result = collection.find({"d": {"$exists": False}}, no_cursor_timeout=True)
                        mon_logger.info("数据查取成功, 共 {} 条".format(result.count()))
                        return result
                    else:
                        mon_logger.info("MongoDB 查取数据为空")
                        return None

                except TypeError as e:
                    mon_logger.error("MongoDB数据查取失败,错误信息为{}, 请检查 {}".format(e, self.mongo_entity_code))
                except pymongo.errors.ServerSelectionTimeoutError as e:
                    mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
                    result = collection.find_one()
                    if result:
                        result = collection.find(no_cursor_timeout=True)
                        mon_logger.info("数据查取成功, 共 {} 条".format(result.count()))
                        return result
                    else:
                        mon_logger.info("MongoDB 查取数据为空")
                        return None

    # 插入数据
    def all_to_mongodb(self, collection, insert_list):
        mon_logger = Logger().logger
        try:
            result = collection.insert_many(insert_list)
            if len(insert_list) != 1:
                mon_logger.info("MongoDB 数据插入成功, 成功条数 {} 条".format(len(result.inserted_ids)))
            return len(result.inserted_ids)

        except TypeError as e:
            mon_logger.info("MongoDB 数据插入失败,错误信息为 {}".format(e))
        except pymongo.errors.ServerSelectionTimeoutError as e:
            mon_logger.info("MongoDB 连接超时 {}, 正在重新连接...".format(e))
            result = collection.insert_many(insert_list)
            if len(insert_list) != 1:
                mon_logger.info("数据插入成功, 成功条数 {} 条".format(len(result.inserted_ids)))
            return len(result.inserted_ids)
        except Exception as e:
            mon_logger.info("MongoDB 数据插入失败,错误信息为 {}".format(e))
            return 0
        finally:
            self.client_close()

    # 删除数据
    def remove_from_mongo(self, collection, remove_id_list):
        # mon_logger = Logger().logger
        # if self.mongo_entity_code:
        #     result = collection.delete_many({"$and": [
        #         {"ENTITY_CODE_": self.mongo_entity_code},
        #         {"_id": {"$in": remove_id_list}}
        #     ]})
        # else:
        result = collection.delete_many({"_id": {"$in": remove_id_list}})

        # mon_logger.info("ENTITY_CODE_: {} 删除成功 {}条".format(self.mongo_entity_code, result.deleted_count))

        return result.deleted_count

    # 更新数据
    def update_to_mongodb(self, collection, data_id, data_dict):
        # mon_logger = Logger().logger
        # try:
        if isinstance(data_id, list):
            result = collection.update_many({"_id": {"$in": data_id}}, {"$set": data_dict})
            # mon_logger.info("ENTITY_CODE_: {} 更新成功 {}条".format(self.mongo_entity_code, result.matched_count))
        else:
            result = collection.update_one({"_id": data_id}, {"$set": data_dict})
        # mon_logger.info("ENTITY_CODE_: {} 更新成功 {}条".format(self.mongo_entity_code, result.matched_count))
        return result.matched_count

        # except Exception as e:
        #     mon_logger.info("ENTITY_CODE_: {} 更新失败: {}".format(self.mongo_entity_code, e))
        #     return None

    def client_close(self):
        self.client.close()

    def main(self):
        db, collection_list = self.client_to_mongodb()
        collection = self.get_check_collection(db=db, collection_list=collection_list)
        result = self.search_from_mongodb(collection)
        # self.client_close()
        return result

    def title_main(self):
        db, collection_list = self.client_to_mongodb()
        collection = self.get_check_collection(db=db, collection_list=collection_list)
        result = self.search_title_from_mongodb(collection)
        # self.client_close()
        return result


if __name__ == '__main__':
    script = MongoClient()
    mo_client=script.client
    # print(script.mongo_host)
    tj=['WD_SS_SQ','WD_SS_XX','WD_SS_YY','WD_TY','JRCP_LCCP','JRCP_XYK','JRCP_BX','ZX_GWDT'
        ,'ZX_HYBG','ZX_ZCGG','ZX_CJXW_HY','WEIBOINFO','mapbar_beijing','mapbar_shanghai','mapbar','WD_JZ_FJ_BJ'
        ,'WD_JZ_FJ_SH','WD_JZ_FJ_NN','WD_JZ_FJ_XM','WD_JZ_FJ_NB','WD_JT_GJ','WD_JT_DT']
    for i in tj:
        collection = mo_client["spider_data"][i]
        data = collection.count()
        print(f"{i}={data}")
    mo_client.close()
    collection.delete_many({"SHUFFLE_TIME_": {"$exists":True}})
    for i in ["WD_JT_DT", "WD_JT_GJ", "WD_JZ_FJ", "WD_SS_SQ", "WD_SS_XX", "WD_SS_YY"]:
        collection = script.db[i]
        print(script.db)
        collection.create_index("SHUFFLE_TIME_")

    # data = collection.find_one_and_update({"d": {"$exists": False}}, {'$set': {"d": 1}})

    # data = collection.find_one({"CONTENT_URL_": "https://m.weibo.cn/detail/4354476443438005"})
    # for each in data["INFO_COMMENTS_"]:
    #     print(each)

