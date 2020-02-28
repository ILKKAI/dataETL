# -*- coding: utf-8 -*-


# 获取当前文件路径
import hashlib
import sys
import os

from scripts import GenericScript

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])


from database._mongodb import MongoClient
from time import sleep
from log.data_log import Logger
import pymongo


class AllToPhoenix(object):
    def __init__(self):
        self.code_list = ["ABCORGANIZE", "BOCOMORGANIZE", "BOCORGANIZE", "CBHBORGANIZE", "CCBORGANIZE", "CEBORGANIZE", "CGBORGANIZE", "CIBORGANIZE", "CMBCORGANIZE", "CMBORGANIZE", "CZBORGANIZE", "EBCLORGANIZE", "ECITICORGANIZE", "HXBORGANIZE", "ICBCORGANIZE", "PABORGANIZE", "PSBCORGANIZE", "SPDBORGANIZE"]
        self.logger = Logger().logger
        self.count = 0

    # 从 MongoDB 获取数据
    def get_data_from_mongo(self, m_client, collection, entity_code):
        m_client.mongo_entity_code = entity_code
        try:
            mongo_data_list = m_client.search_from_mongodb(collection)
            return mongo_data_list
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.info("连接失败，正在重新连接")
            sleep(1)
            mongo_data_list = m_client.search_from_mongodb(collection)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None
        except KeyError as e:
            self.logger.info(e)
            # print(e)
            return None

    # 主函数
    def run(self):
        # 创建 MongoDB 对象
        m_client = MongoClient(mongo_collection="ORGANIZE_FINASSIST")
        db, collection_list = m_client.client_to_mongodb()
        collection = m_client.get_check_collection(db=db, collection_list=collection_list)

        # 遍历 ENTITY_CODE_ 列表
        # self.code_list = self.code_list[14:]
        for entity_code in self.code_list:
            self.count = 0
            hash_list = list()
            status = False
            mongo_data_list = self.get_data_from_mongo(m_client=m_client, collection=collection, entity_code=entity_code)
            if mongo_data_list:
                self.logger.warning("{} 查取成功".format(entity_code))
                self.logger.warning("当前共有{}条".format(mongo_data_list.count()))
                status = True
            else:
               self.logger.warning("{} 无数据".format(entity_code))

            if status:
                for data in mongo_data_list:
                    if "ADDR_" in data:
                        hash_m = hashlib.md5()
                        hash_m.update(data["ADDR_"].encode("utf-8"))
                        hash_title = hash_m.hexdigest()
                        if hash_title in hash_list:
                            self.count += 1
                        else:
                            hash_list.append(hash_title)
                    else:
                        if "CONTENT_" in data:
                            hash_m = hashlib.md5()
                            hash_m.update(data["CONTENT_"].encode("utf-8"))
                            hash_title = hash_m.hexdigest()
                            if hash_title in hash_list:
                                self.count += 1
                            else:
                                hash_list.append(hash_title)
            self.logger.warning("重复数据{}条".format(self.count))

        # 关闭连接
        m_client.client_close()
        self.logger.handlers.clear()


if __name__ == '__main__':
    script_bidding = AllToPhoenix()
    script_bidding.run()
