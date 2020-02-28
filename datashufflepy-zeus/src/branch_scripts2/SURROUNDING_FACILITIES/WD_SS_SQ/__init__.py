# -*- coding: utf-8 -*-
import copy
import os
import sys
import re

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-38])

from branch_scripts2 import GenericScript
from __config import TABLE_NAME
from tools.web_api_of_baidu import get_lat_lng, get_area
from tools.req_for_api import req_for_serial_number
from bs4 import BeautifulSoup


class Branchsssq(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(Branchsssq, self).__init__(table_name, collection_name, param, verify_field=verify_field)

    def generic_shuffle(self, data):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        # different shuffle rule
        re_data_list = list()

        # print(data["CONTENT_HTML_"])

        # 根据CONTENT_HTML_ 获取商圈字典型列表("区域":"商圈名")
        soup = BeautifulSoup(data["CONTENT_HTML_"], "html.parser")
        dl = soup.find_all('dl', {"class": "list"})
        # 商圈字典型列表
        dt_dict = dict()
        for item in dl:
            # print(item)
            dt = item.dt.a.string
            li_list = list()
            for li in item.find_all('li'):
                # print(li)
                li_list.append(li.a.string)
            dt_dict[dt] = li_list
        for area_name in dt_dict:

            shopping_list = dt_dict[area_name]
            # print(dt_dict)

    # 得到各商圈经度和维度 补全省市区域数据
            for shopping_name in shopping_list:
                re_data = dict()

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
                # ID
                serial_number = req_for_serial_number(code="WD_SS_SQ")
                re_data["ID_"] = serial_number
                try:
                    lat_result = get_lat_lng(address=data["CITY_"] + "市" + area_name + shopping_name)
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
                    try:
                        area_result = get_area(",".join([str(re_data["LAT_"]), str(re_data["LNG_"])]))
                    except Exception as e:
                        self.logger.info(f"获取地址失败, ERROR: {e}")
                    else:
                        try:

                            re_data["ADDR_"] = area_result["result"]["formatted_address"]
                            re_data["PROVINCE_NAME_"] = area_result["result"]["addressComponent"]["province"]
                            re_data["CITY_NAME_"] = area_result["result"]["addressComponent"]["city"]
                            re_data["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                            re_data["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                            re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
                            re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
                        except KeyError:
                            re_data["ADDR_"] = shopping_name
                            re_data["PROVINCE_NAME_"] = None
                            re_data["CITY_NAME_"] = data["CITY_"] + "市"
                            re_data["AREA_NAME_"] = None
                            re_data["AREA_CODE_"] = None
                            re_data["CITY_CODE_"] = None
                            re_data["PROVINCE_CODE_"] = None

                re_data["NAME_"] = shopping_name
                re_data = super(Branchsssq, self).generic_shuffle(data=data, re_data=re_data, field=None)
                re_data_list.append({"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data})
        # print(re_data_list)
        return re_data_list


if __name__ == '__main__':
    # param = sys.argv[1]
    param = "{'limitNumber':'20'}"
    verify_field = {'NAME_': 'NAME_'}
    script = Branchsssq(table_name=TABLE_NAME("CHA_BRANCH_BUSINESS"), collection_name="WD_SS_SQ", param=param, verify_field=verify_field)
    script.main()
    script.close_client()
