# -*- coding: utf-8 -*-

# 微信 WECHAT
import hashlib
import json
import re
import sys
import os
import time
from copy import deepcopy
import jaydebeapi
import pymongo
import requests
from lxml.etree import HTML


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])

from log.data_log import Logger
from database._mongodb import MongoClient
from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript

"""
# row_key ENTITY_CODE_ + hashlib(data["TITLE_"])

# "C"
# ID_               HBase row_key
# ENTITY_CODE_      WECHAT
# URL_              ""
# AREA_CODE_        需提取
# BANK_CODE_        re 匹配
# BANK_NAME_        data["BANK_NAME_"]
# UNIT_CODE_        从 ENTITY_NAME_ 提取
# PERIOD_CODE_      data["PERIOD_CODE_"]
# CONTENT_          data["CONTENT_"]
# STATUS_           1
# REMARK_           ""
# CREATE_TIME_      ？data["DATE_TIME_"] 现无
# UPDATE_TIME_      ""

# TITLE_            data["TITLE_"]
# ENTITY_CODE_      "C"
# CONTENT_          "C"

# data["TITLE_"] 为空则为脏数据
"""


class WechatScript(object):
    def __init__(self, entity_type="WECHAT"):
        """
        初始化参数
        :param entity_type: WECHAT
        """
        self.entity_type = entity_type
        self.logger = Logger().logger

        # 创建 Phoenix 对象
        self.p_client = PhoenixHbase(table_name=self.entity_type)
        # 连接 Phoenix
        self.connection = self.p_client.connect_to_phoenix()
        # 创建 MongoDB 对象
        self.m_client = MongoClient(mongo_collection="WECHAT")
        db, collection_list = self.m_client.client_to_mongodb()
        self.collection = self.m_client.get_check_collection(db=db, collection_list=collection_list)

        # # 创建 MongoDB spider_data_old 数据库对象
        # self.old_client = MongoClient(mongo_collection="WECHAT")
        # # 本地测试
        # self.old_client.client = pymongo.MongoClient(host="localhost", port=27017, serverSelectionTimeoutMS=60,
        #                                              connectTimeoutMS=60, connect=False)
        # self.old_client.mongo_db = "spider_data_old"
        # db_old, collection_list_old = self.old_client.client_to_mongodb()
        # self.collection_old = db_old["WECHAT"]

        # 创建 MySQL 对象
        self.mysql_client = GenericScript(entity_code=None, entity_type=None)

        self.remove_id_list = list()
        self.copy_mongo_data_list = list()
        self.verify_list = ["ID_", "ENTITY_CODE_", "URL_", "AREA_CODE_", "BANK_CODE_", "BANK_NAME_", "UNIT_CODE_",
                            "PERIOD_CODE_", "CONTENT_", "CONTENT_TYPE_", "REMARK_", "CREATE_TIME_", "UPDATE_TIME_",
                            "TITLE_", "ENTITY_NAME_", "DEALTIME_", "DATETIME_", "STATUS_", "WECHAT_NAME_", "WECHAT_ID_"]

        # BANK_NAME_ 字典
        self.name_dict = {"ICBC": "中国工商银行", "ABC": "中国农业银行", "BOC": "中国银行", "CCB": "中国建设银行",
                          "BOCOM": "交通银行", "PSBC": "中国邮政储蓄银行", "CZB": "浙商银行", "CBHB": "渤海银行",
                          "ECITIC": "中信银行", "CEB": "中国光大银行", "HXB": "华夏银行", "CMBC": "中国民生银行",
                          "CMB": "招商银行", "CIB": "兴业银行", "CGB": "广发银行", "PAB": "平安银行",
                          "SPDB": "浦发银行", "EBCL": "恒丰银行"}

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Host": "weixin.sogou.com",
            "Referer": "http://weixin.sogou.com/"
        }

        self.url = "http://weixin.sogou.com/weixin?type=1&query={}&ie=utf8&s_from=input&_sug_=y&_sug_type_="

        self.find_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.bad_count = 0
        self.error_count = 0
        self.data_id = ""

    def check_name(self, wechat_id):
        check_dict = dict()
        with open("wechat_id_name.txt", "r", encoding="utf-8")as rf:
            read_data = rf.read()
            if read_data:
                read_data = read_data.replace("\'", "\"")
                read_data = read_data.replace(": None", ": \"None\"")
                # print(read_data)
                check_dict = json.loads(read_data)
            else:
                wechat_name = self.req_for_name(wechat_id)
                check_dict[wechat_id] = wechat_name
                with open("wechat_id_name.txt", "w", encoding="utf-8")as wf:
                    wf.write(str(check_dict))
                return check_dict[wechat_id]
        if wechat_id in check_dict:
            return check_dict[wechat_id]
        else:
            wechat_name = self.req_for_name(wechat_id)
            check_dict[wechat_id] = wechat_name
            with open("wechat_id_name.txt", "w", encoding="utf-8")as wf:
                wf.write(str(check_dict))
            return check_dict[wechat_id]

    def req_for_name(self, wechat_id):
        url = self.url.format(wechat_id)
        # response = WanDou().http_client(url=url, param=self.headers)
        resp1 = requests.get(url=r"http://h.wandouip.com/get/ip-list?pack=853&num=1&xy=1&type=2&lb=\r\n&mr=1&")
        resp2 = resp1.json()["data"][0]
        # print(resp2)
        # resp1.close()
        time.sleep(2)
        try:
            response = requests.get(url=url, headers=self.headers,
                                    proxies={"http": "{}:{}".format(resp2["ip"], resp2["port"])})
        except Exception as e:
            print(1, e)
            self.logger.info("error ip: {}".format(resp2))
            time.sleep(5)
            return self.req_for_name(wechat_id)

        html = HTML(response.content.decode())
        # response.close()
        name = html.xpath('//p[@class="tit"]/a/text()')
        if name:
            # print(name)
            self.error_count = 0
            return name[0]
        else:
            self.error_count += 1
            if self.error_count == 5:
                self.logger.info("wetchat id error: \"{}\"".format(wechat_id))
                return "None"
            else:
                time.sleep(2)
                self.req_for_name(wechat_id)

        # if response is None:
        #     self.logger.info("ip_prox error")
        #     return self.req_for_name(wechat_id)

        # if isinstance(response, str):
        #     html = HTML(response)
        #     name = html.xpath('//p[@class="tit"]/a/text()')
        #     if name:
        #         print(name)
        #         return name[0]
        #     else:
        #         self.logger.info("ip_prox error2")
        #         return self.req_for_name(wechat_id)
        # else:
        #     self.logger.info("ip_prox error change")
        #     return self.req_for_name(wechat_id)

    def data_shuffle(self, data, province_list, city_list, area_list):
        """
        数据清洗
        :param data:
        :param province_list:
        :param city_list:
        :param area_list:
        :return: re_data or None
        """
        # BANK_CODE_正则匹配规则
        pattern = re.compile(r'ICBC|ABC|BOCOM|CCB|BOC|PSBC|CZB|CBHB|ECITIC|CEB|HXB|CMBC|CMB|CIB|CGB|PAB|SPDB|EBCL')

        re_data = dict()

        if data["TITLE_"]:
            # HBase row_key
            hash_m = hashlib.md5()
            hash_m.update(data["TITLE_"].encode("utf-8"))
            hash_title = hash_m.hexdigest()
            row_key = str(data["ENTITY_CODE_"]) + "_" + str(hash_title)

            # "C" 通用列族字段
            re_data["ID_"] = row_key
            re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
            # re_data["URL_"] = ""

            prov_c = None
            prov_n = None
            city_c = None
            city_n = None
            area_c = None
            area_n = None
            bank_n = None

            bank_c = pattern.match(data["ENTITY_CODE_"])
            if bank_c:
                re_data["BANK_CODE_"] = bank_c.group()
            else:
                return None

            # 正则去除银行名称，方便匹配地区编码
            bank_n = re.sub(r"{}银?行?|微信|[总分支]行".format(self.name_dict[re_data["BANK_CODE_"]][:-2]), "",
                            data["ENTITY_NAME_"])

            re_data["BANK_NAME_"] = self.name_dict[re_data["BANK_CODE_"]]
            re_data["PERIOD_CODE_"] = data["PERIOD_CODE_"].replace("-", "")
            re_data["NOTICE_TIME_"] = data["PERIOD_CODE_"]
            re_data["STATUS_"] = "1"
            re_data["CONTENT_"] = data["CONTENT_"]
            re_data["REMARK_"] = ""

            for area in area_list:
                if area["NAME_"] in bank_n:
                    area_c = area["CODE_"]
                    area_n = area["NAME_"]
            if area_c:
                pass
            else:
                for prov in province_list:
                    if prov["NAME_"] in bank_n:
                        prov_c = prov["CODE_"]
                        prov_n = prov["NAME_"]
                        bank_n = bank_n.replace(prov_n, "")
                        break
                    elif prov["NAME_"][:-1] in bank_n:
                        prov_c = prov["CODE_"]
                        prov_n = prov["NAME_"]
                        bank_n = bank_n.replace(prov_n[:-1], "")
                        break
                    elif prov["NAME_"][:4] in bank_n:
                        prov_c = prov["CODE_"]
                        prov_n = prov["NAME_"]
                        bank_n = bank_n.replace(prov_n[:4], "")
                        break
                    elif prov["NAME_"][:3] in bank_n:
                        prov_c = prov["CODE_"]
                        prov_n = prov["NAME_"]
                        bank_n = bank_n.replace(prov_n[:3], "")
                        break
                    elif prov["NAME_"][:2] in bank_n:
                        prov_c = prov["CODE_"]
                        prov_n = prov["NAME_"]
                        bank_n = bank_n.replace(prov_n[:2], "")
                        break

                for city in city_list:
                    if len(city["NAME_"]) == 1:
                        continue
                    if prov_c:
                        if city["CODE_"][:2] == prov_c[:2]:
                            if city["NAME_"] in bank_n:
                                city_c = city["CODE_"]
                                city_n = city["NAME_"]
                                bank_n = bank_n.replace(city_n, "")
                                break
                            elif city["NAME_"][:-1] in bank_n:
                                city_c = city["CODE_"]
                                city_n = city["NAME_"]
                                bank_n = bank_n.replace(city_n[:-1], "")
                                break
                            elif city["NAME_"][:4] in bank_n:
                                city_c = city["CODE_"]
                                city_n = city["NAME_"]
                                bank_n = bank_n.replace(city_n[:4], "")
                                break
                            elif city["NAME_"][:3] in bank_n:
                                city_c = city["CODE_"]
                                city_n = city["NAME_"]
                                bank_n = bank_n.replace(city_n[:3], "")
                                break
                            elif city["NAME_"][:2] in bank_n:
                                city_c = city["CODE_"]
                                city_n = city["NAME_"]
                                bank_n = bank_n.replace(city_n[:2], "")
                                break
                    else:
                        if city["NAME_"] in bank_n:
                            city_c = city["CODE_"]
                            city_n = city["NAME_"]
                            bank_n = bank_n.replace(city_n, "")
                            break
                        elif city["NAME_"][:-1] in bank_n:
                            city_c = city["CODE_"]
                            city_n = city["NAME_"]
                            bank_n = bank_n.replace(city_n[:-1], "")
                            break
                        elif city["NAME_"][:4] in bank_n:
                            city_c = city["CODE_"]
                            city_n = city["NAME_"]
                            bank_n = bank_n.replace(city_n[:4], "")
                            break
                        elif city["NAME_"][:3] in bank_n:
                            city_c = city["CODE_"]
                            city_n = city["NAME_"]
                            bank_n = bank_n.replace(city_n[:3], "")
                            break
                        elif city["NAME_"][:2] in bank_n:
                            city_c = city["CODE_"]
                            city_n = city["NAME_"]
                            bank_n = bank_n.replace(city_n[:2], "")
                            break

                for area in area_list:
                    if city_c:
                        if area["CODE_"][:2] == city_c[:2]:
                            if area["NAME_"] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:-1] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:4] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:3] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:2] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                    elif prov_c:
                        if area["CODE_"][:2] == prov_c[:2]:
                            if area["NAME_"] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:-1] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:4] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:3] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                            elif area["NAME_"][:2] in bank_n:
                                area_c = area["CODE_"]
                                area_n = area["NAME_"]
                                break
                    else:
                        if area["NAME_"][:-1] in bank_n:
                            area_c = area["CODE_"]
                            area_n = area["NAME_"]
                            break
                        elif area["NAME_"][:4] in bank_n:
                            area_c = area["CODE_"]
                            area_n = area["NAME_"]
                            break
                        elif area["NAME_"][:3] in bank_n:
                            area_c = area["CODE_"]
                            area_n = area["NAME_"]
                            break
                        elif area["NAME_"][:2] in bank_n:
                            area_c = area["CODE_"]
                            area_n = area["NAME_"]
                            break

                # 特殊情况 星子县现为庐山市 喻家山位于武汉洪山区
                if "星子县" in data["ENTITY_NAME_"]:
                    area_c = "360483"
                    area_n = "庐山市"
                elif "喻家山" in data["ENTITY_NAME_"]:
                    area_c = "420111"
                    area_n = "洪山区"
                elif "江南西" in data["ENTITY_NAME_"]:
                    area_c = "440105"
                    area_n = "海珠区"
                elif "两路口" in data["ENTITY_NAME_"]:
                    area_c = "500103"
                    area_n = "渝中区"
                elif "大兴安岭" in data["ENTITY_NAME_"]:
                    area_c = "232700"
                    area_n = "大兴安岭地区"
                elif "张家港" in data["ENTITY_NAME_"]:
                    area_c = "320582"
                    area_n = "张家港市"
                elif "兴业银行新阳支行" in data["ENTITY_NAME_"]:
                    area_c = "230102"
                    area_n = "道里区"

                if area_c:
                    pass
                elif (not area_c) and city_c:
                    area_c = city_c
                    area_n = city_n
                elif (not area_c) and (not city_c) and prov_c:
                    area_c = prov_c
                    area_n = prov_n
                # 总行地区处理
                elif (not area_c) and (not city_c) and (not prov_c):
                    if re_data["BANK_CODE_"] == "ICBC":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "ABC":
                        area_c = "110101"
                        area_n = "东城区"
                    elif re_data["BANK_CODE_"] == "BOCOM":
                        area_c = "310115"
                        area_n = "浦东新区"
                    elif re_data["BANK_CODE_"] == "CCB":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "BOC":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "PSBC":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "CZB":
                        area_c = "330103"
                        area_n = "下城区"
                    elif re_data["BANK_CODE_"] == "CBHB":
                        area_c = "120103"
                        area_n = "河西区"
                    elif re_data["BANK_CODE_"] == "ECITIC":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "CEB":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "HXB":
                        area_c = "110101"
                        area_n = "东城区"
                    elif re_data["BANK_CODE_"] == "CMBC":
                        area_c = "110102"
                        area_n = "西城区"
                    elif re_data["BANK_CODE_"] == "CMB":
                        area_c = "440304"
                        area_n = "福田区"
                    elif re_data["BANK_CODE_"] == "CIB":
                        area_c = "350102"
                        area_n = "鼓楼区"
                    elif re_data["BANK_CODE_"] == "CGB":
                        area_c = "440104"
                        area_n = "越秀区"
                    elif re_data["BANK_CODE_"] == "PAB":
                        area_c = "440303"
                        area_n = "罗湖区"
                    elif re_data["BANK_CODE_"] == "SPDB":
                        area_c = "310101"
                        area_n = "黄浦区"
                    elif re_data["BANK_CODE_"] == "EBCL":
                        area_c = "370602"
                        area_n = "芝罘区"
            re_data["AREA_CODE_"] = area_c

            if area_c:
                re_data["UNIT_CODE_"] = re_data["BANK_CODE_"] + "_" + area_c[:4] + "00"
            if ("b" in data["BANK_NAME_"]) or ("B" in data["BANK_NAME_"]):
                return None

            if "DATETIME_" not in data:
                time_array = time.localtime(int(float(data["DEALTIME_"])))
                value_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
                re_data["CREATE_TIME_"] = value_time
            else:
                re_data["CREATE_TIME_"] = data["DATETIME_"]

            # data["UPDATE_TIME_"] = ""

            re_data["TITLE_"] = data["TITLE_"]
            re_data["CONTENT_TYPE_"] = data["CONTENT_TYPE_"]
            re_data["WECHAT_ID_"] = data["WECHAT_"].replace(" ", "")
            re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
            re_data["DEALTIME_"] = str(data["DEALTIME_"])

            # print(area_c, area_n, data["ENTITY_NAME_"])
            return re_data
        else:
            return None

    def delete_data_from_mongo(self):
        """
        从 MongoDB 删除数据
        :return: delete_count
        """
        try:
            remove_count = self.m_client.remove_from_mongo(collection=self.collection,
                                                           remove_id_list=self.remove_id_list)
            return remove_count
        except pymongo.errors.ServerSelectionTimeoutError:
            mongo_data_list = self.m_client.remove_from_mongo(collection=self.collection,
                                                              remove_id_list=self.remove_id_list)
            return mongo_data_list
        except Exception as e:
            self.logger.info(e)
            return None
        except KeyError as e:
            self.logger.info(e)
            return None

    def upsert_and_delete(self, mongo_data_list, province_list, city_list, area_list):
        """
        插入和删除
        :param mongo_data_list:
        :param province_list:
        :param city_list:
        :param area_list:
        :return:
        """
        for i in range(1000000):
            status = False
            self.data_id = ""
            success_count = 0
            try:
                data = mongo_data_list.__next__()
            except StopIteration:
                break
            except pymongo.errors.ServerSelectionTimeoutError as e:
                self.logger.info("MongoDB 超时, 正在重新连接, 错误信息 {}".format(e))
                time.sleep(3)
                data = mongo_data_list.__next__()

            self.data_id = data["_id"]
            if self.success_count % 100 == 0:
                self.logger.info("正在进行 _id 为 {} 的数据".format(self.data_id))
            # print(data["_id"])
            # self.remove_id_list.append(self.data_id)
            # del data["_id"]
            # copy_data = deepcopy(data)
            # self.copy_mongo_data_list.append(copy_data)

            # 清洗数据
            try:
                re_data = self.data_shuffle(data=data, province_list=province_list,
                                            city_list=city_list, area_list=area_list)
            except Exception as e:
                # self.remove_id_list.remove(self.data_id)
                # self.copy_mongo_data_list.remove(copy_data)
                self.logger.info("数据清洗失败 {}, id: {}".format(e, self.data_id))
                continue

            if re_data:
                # 获取公众号名称
                # try:
                # print(re_data["WECHAT_ID_"])
                re_data["WECHAT_NAME_"] = self.check_name(re_data["WECHAT_ID_"])

                # re_data["WECHAT_NAME_"] = self.req_for_name(re_data["WECHAT_ID_"])
                # print(re_data["WECHAT_ID_"])
                # print(re_data["WECHAT_NAME_"])
                # except Exception as e:
                # 向 HBase 插入数据
                try:
                    count = self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=re_data)
                    success_count += count
                except jaydebeapi.DatabaseError as e:
                    # self.logger.info("error: {}".format(e))
                    # self.remove_id_list.remove(self.data_id)
                    # self.copy_mongo_data_list.remove(copy_data)
                    self.logger.info("错误 id: {}, 错误信息 {}".format(self.data_id, e))
                    continue
                    # # Phoenix 连接关闭
                    # p_client.close_client_phoenix(connection=connection)
                    # time.sleep(10)
                    # # 连接 Phoenix
                    # connection = p_client.connect_to_phoenix()
                    # # 向 HBase 插入数据
                    # count = p_client.upsert_to_phoenix_by_one(connection=connection, data=re_data)
                    # success_count += count

                # try:
                #     # 添加 {d:1}
                #     update_count = self.m_client.update_to_mongodb(collection=self.collection, data_id=self.data_id,
                #                                                    data_dict={"d": 1})
                #     self.remove_count += update_count
                #     # self.logger.info("MongoDB 更新成功")
                #     if self.remove_count % 10 == 0:
                #         self.logger.info("MongoDB 更新成功, 成功条数 {} 条".format("10"))
                # except Exception as e:
                #     # self.remove_id_list.remove(data_id)
                #     # self.copy_mongo_data_list.remove(copy_data)
                #     self.logger.warning("MongoDB 更新 _id 为 {} 的数据失败, {}".format(self.data_id, e))
                #     continue

                if success_count > 0:
                    status = True
                    self.success_count += success_count

                if self.success_count % 10 == 0:
                    self.logger.info("HBase 插入成功 {} 条".format(self.success_count))

            else:
                self.bad_count += 1
                # self.remove_id_list.remove(self.data_id)
                # self.copy_mongo_data_list.remove(copy_data)
                continue

            # # 删除数据
            # if status:
            #     delete_count = self.delete_data_from_mongo()
            #     self.remove_count += delete_count
            #     self.logger.info("MongoDB 删除成功")
            # else:
            #     self.logger.info("HBase 插入成功条数0条, 不执行删除")
            #
            # # 将数据插入 spider_data_old 中
            # if status:
            #     try:
            #         self.old_client.mongo_db = "spider_data_old"
            #         insert_count = self.old_client.all_to_mongodb(collection=self.collection_old,
            #                                                       insert_list=self.copy_mongo_data_list)
            #         self.old_count += insert_count
            #         # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
            #     except pymongo.errors.ServerSelectionTimeoutError as e:
            #         time.sleep(1)
            #         self.logger.info("MongoDB 连接失败, 正在重新连接 {}".format(e))
            #         insert_count = self.old_client.all_to_mongodb(collection=self.collection_old,
            #                                                       insert_list=self.copy_mongo_data_list)
            #         self.old_count += insert_count
            #         # self.logger.info("MongoDB 插入成功, 成功条数 {}".format(insert_count))
            #     except Exception as e:
            #         self.logger.info(e)

    def main(self):
        """

        :return:
        """
        # # 删除表
        # self.p_client.drop_table_phoenix(connection=self.connection)
        # # quit()

        # # 建表语句
        # table_sql = ('create table "WECHAT" ("ID_" varchar primary key, "C"."ENTITY_CODE_" varchar,'
        #              '"C"."URL_" varchar, "C"."AREA_CODE_" varchar, "C"."BANK_CODE_" varchar,'
        #              '"C"."BANK_NAME_" varchar, "C"."UNIT_CODE_" varchar, "C"."PERIOD_CODE_" varchar,'
        #              '"C"."REMARK_" varchar, "C"."CREATE_TIME_" varchar, "C"."UPDATE_TIME_" varchar, '
        #              '"T"."CONTENT_" varchar, "C"."CONTENT_TYPE_" varchar, "C"."TITLE_" varchar,'
        #              '"C"."WECHAT_ID_" varchar, "C"."WECHAT_NAME_" varchar, "C"."ENTITY_NAME_" varchar,'
        #              '"C"."DEALTIME_" varchar, "C"."STATUS_" varchar, "C"."PRAISES_" varchar,'
        #              '"C"."READ_NUM_" varchar, "C"."REPLIES_" varchar, "C"."RELAYS_" varchar,'
        #              '"C"."NOTICE_TIME_" varchar, "C"."IMPROTANCE_" varchar) IMMUTABLE_ROWS = true')
        #
        # # 创建表
        # self.p_client.create_new_table_phoenix(connection=self.connection, sql=table_sql)

        # f_id = "5c1267258d7fee59f7d089f8"  # gte 10M
        # f_id = "5c1271a28d7fee66df0fdd83"  # gte 10M
        # f_id = "5c127e7b9bb3df7412b53b04"  # gte 10M
        # f_id = "5c1330d28d7fee4d9c87d6e1"  # gte 10M
        # f_id = "5c1330ed9bb3df2de33bb746"  # gte 10M
        # f_id = "5c13490a8d7fee79f1d9e87f"  # gte 10M
        # f_id = "5c1350ee8d7fee2d29b601ef"  # gte 10M
        # f_id = "5c1351c79bb3df0e23ee68c1"  # gte 10M
        # f_id = "5c13547d9bb3df06d41997d5"  # gte 10M
        # f_id = "5c1354849bb3df202508ee3e"  # gte 10M
        # f_id = "5c1354bd8d7fee44b881b11a"  # gte 10M
        # f_id = "5c1354e89bb3df1b2a6ef59c"  # gte 10M
        # f_id = "5c1355139bb3df197beb11c0"  # gte 10M
        # f_id = "5c1355328d7fee2f0997a3ac"  # gte 10M
        # f_id = "5c13558e8d7fee50ea04bd0a"  # gte 10M
        # f_id = "5c135a5f8d7fee5bf7db91b8"  # gte 10M
        # f_id = "5c135b0c8d7fee697fa5bd80"  # gte 10M
        # f_id = "5c135bd59bb3df4d7aa66cad"  # gte 10M
        # f_id = "5c135bdb9bb3df454c0157a3"  # gte 10M
        # f_id = "5c135bfc8d7fee73c8f84567"  # gte 10M
        # f_id = "5c135c119bb3df48aeb8fe63"  # gte 10M
        # f_id = "5c135dfe9bb3df4d7aa66cc2"  # gte 10M
        # f_id = "5c13602d8d7fee7f7a48c485"  # gte 10M
        # f_id = "5c1361858d7fee223825f805"  # gte 10M
        # f_id = "5c1361d68d7fee561806fc4d"  # gte 10M
        # f_id = "5c1362068d7fee223825f808"  # gte 10M
        # f_id = "5c1362159bb3df26bba60a05"  # gte 10M
        # f_id = "5c1366248d7fee6741adb5be"  # gte 10M
        # f_id = "5c1366418d7fee673f6c95cb"  # gte 10M
        # f_id = "5c1367099bb3df5a0e013c4d"  # gte 10M
        # f_id = "5c13686d8d7fee76ac78735b"  # gte 10M
        # f_id = "5c1368788d7fee6fcb24daa3"  # gte 10M
        # f_id = "5c1369438d7fee63412b04ff"  # gte 10M
        # f_id = "5c13697b9bb3df60429b5d31"  # gte 10M
        # f_id = "5c1389468d7fee6a94c413c3"  # gte 10M
        # f_id = "5c1389c29bb3df75adc8861a"  # gte 10M
        # f_id = "5c138b039bb3df75adc88620"  # gte 10M
        # f_id = "5c138e3d9bb3df074c4ec0b3"  # gte 10M
        # f_id = "5c138e4d8d7fee06a4f8fd59"  # gte 10M
        # f_id = "5c1391318d7fee168749a96e"  # gte 10M
        # f_id = "5c25a4f19bb3df51eba386b8"  # gte 10M
        # f_id = "5c2601ef9bb3df7d42fe2084"  # gte 10M
        # f_id = "5c2608099bb3df24f5db4527"  # gte 10M
        # f_id = "5c2608be9bb3df2d58d08e32"  # gte 10M
        # f_id = "5c260d2b9bb3df3c084d2a83"  # gte 10M
        # f_id = "5c2615868d7fee2771bb3914"  # gte 10M
        # f_id = "5c261d528d7fee3c1383db85"  # gte 10M
        # f_id = "5c26340e8d7fee66d784fe8a"  # gte 10M
        # f_id = "5c263b818d7fee630f0d3ac4"  # gte 10M
        # f_id = "5c263ee28d7fee04ddc62e31"  # gte 10M
        # f_id = "5c263f269bb3df0d29d1e1e5"  # gte 10M
        # f_id = "5c2766718d7fee2aa36fa166"  # gte 10M
        # f_id = "5c2b79ef8d7fee3025e02575"  # gte 10M
        # f_id = "5c2b854a9bb3df27dc669d5a"  # gte 10M
        # f_id = "5c2e00078d7fee1b60443cf3"  # gte 10M
        # f_id = "5c2f69028d7fee62d31a72db"  # gte 10M
        # f_id = "5c36a7948d7fee18d9333327"  # gte 10M
        # f_id = "5c36b9ff9bb3df332dfebe39"  # gte 10M
        # f_id = "5c3754579bb3df02b680150b"  # gte 10M
        # f_id = "5c375c969bb3df6afd18e22d"  # gte 10M
        # f_id = "5c38a1e59bb3df6b2ff2f269"  # gte 10M
        # f_id = "5c394e058d7fee6a2582d1d3"  # gte 10M
        # f_id = "5c3c983e9bb3df21ddf94a92"  # gte 10M
        # f_id = "5c3ca38a9bb3df60bca07833"  # gte 10M

        f_id = "5c3c983e9bb3df21ddf94a92"
        # f_id = ""
        self.data_id = f_id

        province_list, city_list, area_list, dir_area_list = self.mysql_client.area_from_mysql()
        mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection, data_id=self.data_id)
        self.find_count += mongo_data_list.count()

        try:
            self.upsert_and_delete(mongo_data_list=mongo_data_list, province_list=province_list,
                                   city_list=city_list, area_list=area_list)
        except jaydebeapi.DatabaseError:
            self.logger.info("error id is: {}".format(self.data_id))
            mongo_data_list = self.m_client.all_from_mongodb(collection=self.collection, data_id=self.data_id)
            self.upsert_and_delete(mongo_data_list=mongo_data_list, province_list=province_list,
                                   city_list=city_list, area_list=area_list)

        self.logger.info("本次共向 MongoDB 查取数据{}条".format(self.find_count))
        self.logger.info("本次共向 HBase 插入数据{}条".format(self.success_count))
        self.logger.info("本次共向 MongoDB 删除数据{}条".format(self.remove_count))
        self.logger.info("本次共向 MongoDB 插入数据{}条".format(self.old_count))
        self.logger.info("本次坏数据共 {} 条".format(self.bad_count))
        self.logger.handlers.clear()


if __name__ == '__main__':
    script = WechatScript(entity_type="WECHAT")
    script.main()

    # with ThreadPoolExecutor(2) as executor:
    #     all_task = list()
    #     batch_list = []
    #     try:
    #         while True:
    #             batch_list = next(batch_)
    #             all_task.append(executor.submit(main.data_to_hbase, batch_list))
    #             for future in as_completed(all_task):
    #                 data = future.result()
    #                 if data is not None:
    #                     print(data)
    #                 # else:
    #                     # print("数据插入失败，正在重新插入，{}".format(data))
    #     except Exception:
    #         all_task.append(executor.submit(main.data_to_hbase, batch_list))
