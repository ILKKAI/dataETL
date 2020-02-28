# -*- coding: utf-8 -*-
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


class WeiboBasicInfoUpdate(object):
    def __init__(self, table_name="CHA_BRANCH_WEIBO_BASIC", collection_name="WEIBOBASICINFO"):
        # phoenix connection
        self.p_client = PhoenixHbase(table_name=table_name)
        self.connection = self.p_client.connect_to_phoenix()
        # Mongo connection
        self.m_client = MongoClient(entity_code="CMBCMICROBLOG", mongo_collection=collection_name)
        self.mongo_host = "172.22.69.35"
        self.mongo_port = 20000
        self.m_client.client = pymongo.MongoClient(host="172.22.69.35", port=20000, serverSelectionTimeoutMS=60,
                                                   connectTimeoutMS=60, connect=False)
        self.db, self.collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=self.db, collection_list=self.collection_list)
        # Log
        self.logger = Logger().logger

    def get_mongo_column_dict(self, collection, column1, column2):
        mon_logger = Logger().logger
        try:
            mon_logger.info("开始查取数据")
            result = collection.aggregate([{"$project": {"_id": 0, column1: 1, column2: 1}}])
            return result
        except TypeError as e:
            mon_logger.error("WEIBO_CODE_ 数据查取失败,错误信息为{}, 请检查匹配规则是否正确".format(e))
            raise Exception("WEIBO_CODE_ 查取失败, 错误信息为{}".format(e))

        finally:
            self.m_client.client.close()

    def main(self):
        mongo_data_list = self.get_mongo_column_dict(collection=self.collection, column1="WEIBO_CODE_", column2="FANS_")

        # update to hbase

        result_generator = self.p_client.search_all_from_phoenix(connection=self.connection, dict_status=True)
        while True:
            try:
                result = result_generator.__next__()
                for mongo_data in mongo_data_list:
                    if mongo_data["WEIBO_CODE_"] == result["WEIBO_CODE_"]:
                        result["FANS_"] = mongo_data["FANS_"]
                        break
            except StopIteration:
                break
            self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=result)
        self.connection.close()


if __name__ == '__main__':
    script = WeiboBasicInfoUpdate()
    script.main()