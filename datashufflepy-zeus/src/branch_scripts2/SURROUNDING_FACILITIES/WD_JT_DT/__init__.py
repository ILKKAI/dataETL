# -*- coding: utf-8 -*-
import os
import sys
import re

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-38])

from branch_scripts2 import GenericScript
from __config import TABLE_NAME
from tools.web_api_of_baidu import get_lat_lng, get_area, get_periphery
from tools.req_for_api import req_for_serial_number
from tools.req_for_ai import req_for_textLoc
from copy import deepcopy


class Branchjtdt(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(Branchjtdt, self).__init__(table_name, collection_name, param, verify_field=verify_field)

    def generic_shuffle(self, data):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        # different shuffle rule
        re_data = dict()
        # ID
        serial_number = req_for_serial_number(code="WD_JT_DT")
        re_data["ID_"] = serial_number
        # 时间维度
        re_data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
        # 标签
        if "TAGS_" in data:
            re_data["TAGS_"] = ""
        # SOURCE
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        re_data["SOURCE_"] = source[0]
        # 数据来源名称
        re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"].split("-")[0]
        # # 数据来源编码
        # s_index = data["ENTITY_CODE_"].rfind("_")
        # re_data["SOURCE_CODE_"] = data["ENTITY_CODE_"][:s_index]
        # 资讯来源分类
        re_data["SOURCE_TYPE_"] = data["ENTITY_CODE_"][3:8]
        # 得到经度和维度 补全省市区域数据
        temp_location = data["SUBWAY_NAME_"][:data["SUBWAY_NAME_"].find("|")] + data["STATION_NAME_"]+"地铁站"
        # print(temp_location)
        # try:
        #     res = req_for_textLoc(text=data["SUBWAY_NAME_"][:data["SUBWAY_NAME_"].find("|")] + data["STATION_NAME_"]+"地铁站")
        # except Exception as e:
        #     self.logger.exception(f"2.2--err: 请求模型 req_for_textLoc 错误."
        #                           f" 原始数据 collection = {self.m_client.mongo_collection};"
        #                           f" ENTITY_CODE_ = {self.entity_code};"
        #                           f" 原始数据 _id = {data['_id']};"
        #                           f" error: {e}.")
        # else:
        #     if "error" not in res:
        #         if res["tagsId"] == "None" or res["tagsId"] is None:
        #             pass
        #         else:
        #             re_data["TAGS_"] = res["tagsId"]
        #         if res["flag"] == 1:
        try:
            lat_result = get_lat_lng(address=temp_location)
            re_data["LAT_"] = lat_result["result"]["location"]["lat"]
            re_data["LNG_"] = lat_result["result"]["location"]["lng"]
        except KeyError:
            re_data["LAT_"] = None
            re_data["LNG_"] = None
        except Exception as e:
            re_data["LAT_"] = None
            re_data["LNG_"] = None
            self.logger.info("获取经纬度失败错误信息为{}".format(e))
        if re_data["LAT_"]:
            lat_handle = ""
            try:
                lat_origin = ",".join([str(re_data["LAT_"]), str(re_data["LNG_"])])
                i = 0
                find_tag = False
                while True:
                    s3 = get_periphery(classify="地铁站", tag="交通设施", lat_lng=lat_origin, radius=3000, page_num=i)
                    for nearby in s3["results"]:
                        if nearby["name"] == data["STATION_NAME_"]:
                            find_tag = True
                            lat = str(nearby["location"]["lat"])
                            lng = str(nearby["location"]["lng"])
                            re_data["LAT_"] = lat
                            re_data["LNG_"] = lng
                            lat_handle = lat + "," + lng
                            break
                    if find_tag:
                        break
                    i +=1
                    if len(s3["results"]) != 20:
                        break
            except Exception as e:
                self.logger.info(f"获取精确经纬度失败, ERROR: {e}")
            if len(lat_handle) > 0:
                # 获取精确经纬度后根据精确经纬度补全地址信息
                try:
                    # area_result = get_area(",".join([str(re_data["LAT_"]), str(re_data["LNG_"])]))
                    area_result = get_area(lat_handle)
                except Exception as e:
                    self.logger.info(f"获取地址失败, ERROR: {e}")
                else:
                    try:
                        re_data["PROVINCE_NAME_"] = area_result["result"]["addressComponent"]["province"]
                        re_data["CITY_NAME_"] = area_result["result"]["addressComponent"]["city"]
                        re_data["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                        re_data["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                        re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
                        re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
                    except KeyError:
                        pass
            else:
                try:
                    area_result = get_area(",".join([str(re_data["LAT_"]), str(re_data["LNG_"])]))
                except Exception as e:
                    self.logger.info(f"获取地址失败, ERROR: {e}")
                else:
                    try:
                        re_data["PROVINCE_NAME_"] = area_result["result"]["addressComponent"]["province"]
                        re_data["CITY_NAME_"] = area_result["result"]["addressComponent"]["city"]
                        re_data["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                        re_data["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                        re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
                        re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
                    except KeyError:
                        pass
        # print(re_data)
        # 站点名称
        if "STATION_NAME_" in data:
            re_data["STATION_NAME_"] = data["STATION_NAME_"]
        # 途经路线(地铁几号线）
        temp_subway = data["SUBWAY_NAME_"].replace("|", "-")
        AROUND_ROUTE_ = re.findall(r"地铁\d+号线", temp_subway)
        if len(AROUND_ROUTE_) == 1:
            re_data["AROUND_ROUTE_"] = AROUND_ROUTE_[0]
        elif len(AROUND_ROUTE_) > 1:
            re_data["AROUND_ROUTE_"] = ",".join(AROUND_ROUTE_)
        else:
            re_data["AROUND_ROUTE_"] = ""

        # 地铁名称
        if "SUBWAY_NAME_" in data:
            SUBWAY_NAME_ = data["SUBWAY_NAME_"].replace("|", "-")
            if "," in SUBWAY_NAME_:
                re_data_list = list()
                SUBWAY_LIST = SUBWAY_NAME_.split(",")
                for subway in SUBWAY_LIST:
                    # 拆开的地铁名称需要再获取serial_number
                    serial_number = req_for_serial_number(code="WD_JT_DT")
                    re_data["ID_"] = serial_number
                    re_data["SUBWAY_NAME_"] = subway + "-" + re_data["STATION_NAME_"]
                    re_data = super(Branchjtdt, self).generic_shuffle(data=data, re_data=re_data, field=None)
                    # temp_dict = deepcopy(re_data)
                    temp_dict = deepcopy({"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data})
                    re_data_list.append(temp_dict)
                return re_data_list
            else:
                re_data["SUBWAY_NAME_"] = SUBWAY_NAME_ + "-" + re_data["STATION_NAME_"]
                re_data = super(Branchjtdt, self).generic_shuffle(data=data, re_data=re_data, field=None)
                return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]


if __name__ == '__main__':
    param = sys.argv[1]
    # param = "{'limitNumber':'1000'}"
    verify_field = {'SUBWAY_NAME_': 'SUBWAY_NAME_'}
    script = Branchjtdt(table_name=TABLE_NAME("CHA_BRANCH_SUBWAY"), collection_name="WD_JT_DT", param=param, verify_field=verify_field)
    script.main()
    script.close_client()
    # filelist = ['WD_JT_DT_BDDT_BJ',
    # 'WD_JT_DT_BDDT_CD',
    # 'WD_JT_DT_BDDT_NB',
    # 'WD_JT_DT_BDDT_NN',
    # 'WD_JT_DT_BDDT_SH',
    # 'WD_JT_DT_BDDT_XM']
    # for i in filelist:
    #     with open("{}.py".format(i), "w") as f:
    #         f.write("from database._mongodb import MongoClient\n\n\ndef data_shuffle(data):\n\n    return data\n\n\nif __name__ == '__main__':\n    main_mongo = MongoClient(entity_code=\"{}\", mongo_collection=\"WD_JT_DT\")".format(i))