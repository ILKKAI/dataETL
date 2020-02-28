import os
import sys
import re

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-38])

from tools.req_for_api import req_for_serial_number
from branch_scripts2 import GenericScript
from tools.web_api_of_baidu import get_lat_lng, get_area
from __config import TABLE_NAME


class Branchssyy(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(Branchssyy, self).__init__(table_name, collection_name, param, verify_field=verify_field)

    def generic_shuffle(self, data):
        """
        清洗脚本写到这里
        :param data:
        :return re_data:
        """
        re_data = dict()
        serial_number = req_for_serial_number(code="WD_SS_YY")
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
        try:
            lat_result = get_lat_lng(address=data["ADDR_"])
            re_data["LAT_"] = lat_result["result"]["location"]["lat"]
            re_data["LNG_"] = lat_result["result"]["location"]["lng"]
        except KeyError:
            try:
                lat_result = get_lat_lng(address=data["CITY_NAME_"]+data["NAME_"])
                re_data["LAT_"] = lat_result["result"]["location"]["lat"]
                re_data["LNG_"] = lat_result["result"]["location"]["lng"]
            except KeyError:
                re_data["LAT_"] = None
                re_data["LNG_"] = None
            except Exception as e:
                re_data["LAT_"] = None
                re_data["LNG_"] = None
                self.logger.info("获取经纬度失败错误为{}".format(e))
        except Exception as e:
            re_data["LAT_"] = None
            re_data["LNG_"] = None
            self.logger.info("获取经纬度失败错误为{}".format(e))
        if re_data["LNG_"]:
            try:
                area_result = get_area(",".join([str(re_data["LAT_"]), str(re_data["LNG_"])]))
            except Exception as e:
                self.logger.info("获取地址信息失败错误为{}".format(e))
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

        # 设备
        if "DEVICE_" in data:
            re_data["DEVICE_"] = data["DEVICE_"]
        # 医院等级
        if "GRADE_" in data:
            re_data["GRADE_"] = data["GRADE_"]
        # 特色
        if "SPECIAL_" in data:
            re_data["SPECIAL_"] = data["SPECIAL_"]
        # 电话
        if "TEL_" in data:
            re_data["TEL_"] = data["TEL_"]
        # 医院id
        if "HOSPITAL_ID_" in data:
            re_data["HOSPITAL_ID_"] = data["HOSPITAL_ID_"]
        # 医院名称
        if "NAME_" in data:
            re_data["NAME_"] = data["NAME_"]
        # 地址
        if "ADDR_" in data:
            re_data["ADDR_"] = data["ADDR_"]
        # 床位
        if "BEDS_" in data:
            re_data["BEDS_"] = data["BEDS_"]
        # 医院性质
        if "TYPE_" in data:
            re_data["TYPE_"] = data["TYPE_"]
        # 网站
        if "WEBSITE_" in data:
            re_data["WEBSITE_"] = data["WEBSITE_"]
        # 门诊量
        if "VOLNUM_" in data:
            re_data["VOLNUM_"] = data["VOLNUM_"]
        # print(re_data)
        re_data = super(Branchssyy, self).generic_shuffle(data=data, re_data=re_data, field=None)
        return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]


if __name__ == '__main__':
    param = sys.argv[1]
    # param = "{'limitNumber':'1000'}"
    verify_field = {'URL_': 'URL_'}
    script = Branchssyy(table_name=TABLE_NAME("CHA_BRANCH_HOSPITAL"), collection_name="WD_SS_YY", param=param,
                        verify_field=verify_field)
    script.main()
    script.close_client()
