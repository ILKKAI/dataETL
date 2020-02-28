# -*- coding: utf-8 -*-
"""链家通用清洗脚本"""
import os
import re
import sys
import time
import arrow


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-38])

from tools.req_for_api import req_for_something, req_for_serial_number
from tools.web_api_of_baidu import get_lat_lng, get_area
from branch_scripts2 import GenericScript
from tools.req_for_ai import req_for_textLoc
from __config import TABLE_NAME, CREATE_ID, CREATE_NAME
from database._phoenix_hbase import value_replace


class BranchNews(GenericScript):
    def __init__(self, table_name, collection_name, param):
        # if "LISPMM" in param or "LIXZLMM" in param or "LJXQFJ" in param or "LISPZL" in param:
        #     verify_field = {"ENTITY_CODE_": "ENTITY_CODE_", "URL_": "URL_"}
        # else:
        verify_field = {"ENTITY_CODE_": "ENTITY_CODE_", "URL_": "URL_"}
        super(BranchNews, self).__init__(table_name=table_name, collection_name=collection_name, param=param,
                                         verify_field=verify_field)
        # self.script_path = self.m_client.mongo_collection
        self.data_table_name = table_name
        self.base_table_name = table_name[:-5]

    def if_exists(self, name, city_name):
        where_condition = f"NAME_ = '{name}' and CITY_NAME_ = '{city_name}'"
        result = self.p_client.search_all_from_phoenix(connection=self.connection,
                                                       table_name=self.base_table_name,
                                                       output_field="ID_",
                                                       iter_status=False, where_condition=where_condition)
        try:
            house = iter(result).__next__()
        except StopIteration:
            return
        else:
            return house[0]

    def generic_shuffle(self, data):
        # print(data)
        re_data = dict()
        # 通用字段
        # ID_  历史信息 ID_
        serial_number = req_for_serial_number(code="WD_JZ_FJ_DATA")
        re_data["ID_"] = serial_number
        re_data["URL_"] = data["URL_"]
        # 时间维度
        re_data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
        # 实体编码、名称及 url
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
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
        # 状态
        if "DATA_STATUS_" not in re_data:
            re_data["DATA_STATUS_"] = "UNCHECK"
        if "PUBLISH_STATUS_" not in re_data:
            re_data["PUBLISH_STATUS_"] = "N"
        # 名称
        re_data["NAME_"] = data["NAME_"].replace("|", "")
        # 类型: 住宅(ZZ)、写字楼(XZL)、商铺(SP)
        if "LISP" in data["ENTITY_CODE_"]:
            re_data["TYPE_"] = "SP"
        elif "LIXQ" in data["ENTITY_CODE_"] or "LJXQ" in data["ENTITY_CODE_"]:
            re_data["TYPE_"] = "ZZ"
        elif "LIXZL" in data["ENTITY_CODE_"]:
            re_data["TYPE_"] = "XZL"

        # 验证名称是否在基本表中
        verify_name = value_replace(re_data["NAME_"])
        house_id = self.if_exists(name=verify_name, city_name="石家庄市")

        # 基本表存在, 只插入 DATA 表
        if house_id:
            re_data["P_ID_"] = house_id
            if "TITLE_" in data:
                re_data["TITLE_"] = data["TITLE_"].replace("|", "")
            if "PUBLISH_TIME_" in data:
                re_data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"]
            else:
                re_data["PUBLISH_TIME_"] = data["DATETIME_"][:10]
            price = re.findall(r"[\d.]+", data["PRICE_"])
            if price:
                re_data["PRICE_"] = price[0]
            else:
                re_data["PRICE_"] = 0
            if "租赁" in data["ENTITY_NAME_"]:
                re_data["USE_TYPE_"] = "RENT"
            else:
                re_data["USE_TYPE_"] = "SALE"

            return [{"TABLE_NAME_": self.data_table_name, "DATA_": re_data}]
        else:
            # 基本信息表ID_
            base_id = req_for_serial_number(code="WD_JZ_FJ_BASE")
            # DATA_ 表
            data_dict = dict()
            data_dict.update(re_data)
            data_dict["P_ID_"] = base_id
            if "TITLE_" in data:
                data_dict["TITLE_"] = data["TITLE_"].replace("|", "")
            if "PUBLISH_TIME_" in data:
                data_dict["PUBLISH_TIME_"] = data["PUBLISH_TIME_"]
            else:
                data_dict["PUBLISH_TIME_"] = data["DATETIME_"][:10]
            price = re.findall(r"[\d.]+", data["PRICE_"])
            if price:
                data_dict["PRICE_"] = price[0]
            else:
                data_dict["PRICE_"] = 0
            if "租赁" in data["ENTITY_NAME_"]:
                data_dict["USE_TYPE_"] = "RENT"
            else:
                data_dict["USE_TYPE_"] = "SALE"
            # 基本信息表
            basic_dict = dict()
            basic_dict.update(re_data)
            basic_dict["ID_"] = base_id
            basic_dict["URL_"] = data["URL_"]
            basic_dict["PROVINCE_CODE_"] = "1300"
            basic_dict["PROVINCE_NAME_"] = "河北省"
            basic_dict["CITY_CODE_"] = "130100"
            basic_dict["CITY_NAME_"] = "石家庄市"
            # JAVA 计算
            basic_dict["SALE_PRICE_"] = 0
            basic_dict["RENT_PRICE_"] = 0
            if "YEAR_" in data:
                year = re.findall(r"\d+", data["YEAR_"])
                if year:
                    basic_dict["YEAR_"] = year[0]

            # 地址分析
            try:
                if basic_dict["PROVINCE_NAME_"] == basic_dict["CITY_NAME_"]:
                    basic_dict["ADDR_"] = basic_dict["PROVINCE_NAME_"] + basic_dict["NAME_"]
                else:
                    basic_dict["ADDR_"] = basic_dict["PROVINCE_NAME_"] + basic_dict["CITY_NAME_"] + basic_dict["NAME_"]
                res = req_for_textLoc(text=basic_dict["ADDR_"])
            except Exception as e:
                self.logger.exception(f"2.2--err: 请求模型 req_for_textLoc 错误."
                                      f" 原始数据 collection = {self.m_client.mongo_collection};"
                                      f" ENTITY_CODE_ = {self.entity_code};"
                                      f" 原始数据 _id = {data['_id']};"
                                      f" error: {e}.")
            else:
                if "error" not in res:
                    if res["tagsId"] == "None" or res["tagsId"] is None:
                        pass
                    else:
                        basic_dict["TAGS_"] = res["tagsId"]
                    if res["flag"] == 1:
                        basic_dict["ADDR_"] = res["full"]
                    else:
                        basic_dict["ADDR_"] = data["ADDR_"]
                    try:
                        lat_result = get_lat_lng(address=basic_dict["ADDR_"])
                        basic_dict["LAT_"] = lat_result["result"]["location"]["lat"]
                        basic_dict["LNG_"] = lat_result["result"]["location"]["lng"]
                    except KeyError:
                        basic_dict["LAT_"] = None
                        basic_dict["LNG_"] = None
                    except Exception as e:
                        self.logger.info(f"获取经纬度失败, ERROR: {e}")
                        basic_dict["LAT_"] = None
                        basic_dict["LNG_"] = None
                    if basic_dict["LAT_"]:
                        try:
                            area_result = get_area(",".join([str(basic_dict["LAT_"]), str(basic_dict["LNG_"])]))
                        except Exception as e:
                            self.logger.info(f"获取地址失败, ERROR: {e}")
                        else:
                            try:
                                basic_dict["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                                basic_dict["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                            except KeyError:
                                pass
                            try:
                                basic_dict["ADDR_"] = area_result["result"]["formatted_address"]
                            except KeyError:
                                pass
            # basic_dict["AREA_CODE_"] = data[""]
            #             # basic_dict["AREA_NAME_"] = data[""]
            #             # basic_dict["LAT_"] = data[""]
            #             # basic_dict["LNG_"] = data[""]
            #             # basic_dict["BANK_CODE_"] = data[""]
            #             # basic_dict["BANK_NAME_"] = data[""]
            #             # basic_dict["REMARK_"] = data[""]
            basic_dict["M_STATUS_"] = "N"
            basic_dict["DELETE_STATUS_"] = "N"
            # basic_dict["TAGS_"] = data[""]
            # 数据来源 URL
            source = re.findall(r"(https?://.*?)/", data["URL_"])
            re_data["SOURCE_"] = source[0]
            # 数据来源 网站名称
            re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"].split("-")[0]
            basic_dict["SOURCE_TYPE_"] = "链家"
            # basic_dict["PRICE_TYPE_"] = data[""]
            basic_dict["ADDR_"] = data["ADDR_"]

            return [{"TABLE_NAME_": self.data_table_name, "DATA_": data_dict},
                    {"TABLE_NAME_": self.base_table_name, "DATA_": basic_dict}]


if __name__ == '__main__':
    # try:
    #     param = sys.argv[1]
    # except Exception:
    #     param = "{}"
    '''
    # WD_JZ_FJ_LJXQFJ_SJZ
    # WD_JZ_FJ_LIXZLMM_SJZ
    # WD_JZ_FJ_LIXZLZL_SJZ
    # WD_JZ_FJ_LISPMM_SJZ
    WD_JZ_FJ_LISPZL_SJZ
    WD_JZ_FJ_LIXQZL_SJZ
    '''
    param = "{'entityType':'WD_JZ_FJ_SJZ','limitNumber':100000,'entityCode':['WD_JZ_FJ_LIXQZL_SJZ']}"
    script = BranchNews(table_name=TABLE_NAME("CHA_BRANCH_HOUSE_DATA"), collection_name="WD_JZ_FJ_SJZ", param=param)
    script.main()
    script.close_client()
