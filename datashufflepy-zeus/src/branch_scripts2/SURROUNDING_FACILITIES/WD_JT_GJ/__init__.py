# -*- coding: utf-8 -*-
import sys
import os
import re
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-38])

from branch_scripts2 import GenericScript
from tools.web_api_of_baidu import get_area, get_lat_lng, get_periphery
from tools.req_for_api import req_for_serial_number
from __config import TABLE_NAME


class Branchjtgj(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(Branchjtgj, self).__init__(table_name, collection_name, param, verify_field=verify_field)

    # 去除首尾的|和连续的||
    def handle_special_text(self, text):
        expr1 = r"^\|"
        text = re.sub(expr1, "", text)
        expr2 = r"\|+"
        text = re.sub(expr2, "|", text)
        expr3 = r"\|$"
        text = re.sub(expr3, "", text)
        return text

    def generic_shuffle(self, data):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :param data:
        :return:
        """

        re_data = dict()
        serial_number = req_for_serial_number(code="WD_JT_GJ")
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
        # # 补全经度纬度和省市等信息
        # try:
        #     city = data["ENTITY_NAME_"][data["ENTITY_NAME_"].rfind("-")+1:]
        #     lat_result_list = get_infomation(data["NAME_"], city)
        #     print(lat_result_list)
        # except KeyError:
        #     re_data["LAT_"] = None
        #     re_data["LNG_"] = None
        # except Exception as e:
        #     re_data["LAT_"] = None
        #     re_data["LNG_"] = None
        #     self.logger.info("获取经纬度失败{}".format(e))
        # if lat_result_list.get('result') and len(lat_result_list['result']) > 0:
        #     for lat_result in lat_result_list['result']:
        #         if lat_result["name"] == "{}-公交车站".format(data["NAME_"]):
        #             print("找到公交")
        #             re_data["LAT_"] = lat_result["location"]["lat"]
        #             re_data["LNG_"] = lat_result["location"]["lng"]
        #             break

        temp_location = data["ENTITY_NAME_"][data["ENTITY_NAME_"].rfind("-")+1:] + data["NAME_"] + "公交车站"
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
        if re_data.get("LAT_"):
            # 根据前面查询的经纬度获取周围公交车站精确经纬度
            lat_handle = ""
            try:
                lat_origin = ",".join([str(re_data["LAT_"]), str(re_data["LNG_"])])
                i = 0
                find_tag = False
                while True:
                    s3 = get_periphery(classify="公交车站", tag="交通设施", lat_lng=lat_origin, radius=3000, page_num=i)
                    for nearby in s3["results"]:
                        if data["NAME_"] in nearby["name"]:
                            find_tag = True
                            lat = str(nearby["location"]["lat"])
                            lng = str(nearby["location"]["lng"])
                            re_data["LAT_"] = lat
                            re_data["LNG_"] = lng
                            lat_handle = lat + "," + lng
                            break
                    if find_tag:
                        break
                    i += 1
                    if len(s3["results"]) != 20:
                        break
            except Exception as e:
                self.logger.info(f"获取精确经纬度失败, ERROR: {e}")
            if len(lat_handle) > 0:
                # 获取精确经纬度后根据精确经纬度补全地址信息
                try:
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

        # 站点描述
        re_data["DESCRIBE_"] = data["DESCRIBE_"]
        # 周边站点
        re_data["AROUND_STATIONS_"] = self.handle_special_text(data["AROUND_STATIONS_"]).replace("|", ",")
        # 途径路线
        re_data["AROUND_ROUTE_"] = self.handle_special_text(data["AROUND_ROUTE_"]).replace("|", ",")
        if re_data["AROUND_ROUTE_"]:
            re_data["AROUND_ROUTE_"] = re_data["AROUND_ROUTE_"].replace("公交线路", "")
        # 站点名称
        re_data["NAME_"] = data["NAME_"]
        re_data = super(Branchjtgj, self).generic_shuffle(data=data, re_data=re_data, field=None)

        return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]


if __name__ == '__main__':
    param = sys.argv[1]
    verify_field = {'URL_': 'URL_'}
    # param = "{'limitNumber':'10000'}"
    script = Branchjtgj(table_name=TABLE_NAME("CHA_BRANCH_BUS_STATION"), collection_name="WD_JT_GJ", param=param,
                        verify_field=verify_field)
    script.main()
    script.close_client()
