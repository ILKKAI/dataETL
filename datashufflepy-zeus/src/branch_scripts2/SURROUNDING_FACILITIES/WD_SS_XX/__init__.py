import re
import os
import sys
import base64
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-38])

from tools.req_for_api import req_for_serial_number,req_for_something
from tools.web_api_of_baidu import get_area, get_lat_lng
from branch_scripts2 import GenericScript
from __config import TABLE_NAME


class Branchssxx(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(Branchssxx, self).__init__(table_name, collection_name, param, verify_field=verify_field)

    def generic_shuffle(self, data):
        """
        清洗脚本写到这里
        :param data:
        :return re_data:
        """

        re_data = dict()
        serial_number = req_for_serial_number(code="WD_SS_XX")
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
            re_data["LAT_"] = None
            re_data["LNG_"] = None
        except Exception as e:
            self.logger.info("获取经纬度失败信息为{}".format(e))
        if re_data["LAT_"]:
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

        # 学校名称
        if "NAME_" in data:
            re_data["NAME_"] = data["NAME_"]
        # 属性(市重点、区重点、全国重点)
        if "LEVEL_" in data:
            re_data["LEVEL_"] = data["LEVEL_"]
        # 图片
        if "IMAGES_" in data:
            if data["IMAGES_"]:
                response = req_for_something(url=data["IMAGES_"])
                if response:
                    t = base64.b64encode(response.content)
                    re_data["IMAGES_"] = t.decode("utf-8")
        # 学校类型
        if "SCHOOL_TYPE_" in data:
            re_data["SCHOOL_TYPE_"] = data["SCHOOL_TYPE_"]
        # 学校性质
        if "SCHOOL_NATURE_" in data:
            re_data["SCHOOL_NATURE_"] = data["SCHOOL_NATURE_"]
        # 电话
        if "TEL_" in data:
            pattern1 = re.compile(r"(\d{3,4}-\d{8})(\d{3,4}-\d{8})")
            pattern2 = re.compile(r"(\d{3,4}-\d{8})(\d{8})")
            pattern3 = re.compile(r"(\d{3,4}-\d{8})(\d{11})")
            pattern4 = re.compile(r"(\d{3,4}-\d{8})(\d{8})(\d{8})")
            pattern5 = re.compile(r"(\d{8})(\d{11})")
            pattern6 = re.compile(r"(\d{8})(\d{8})")
            pattern7 = re.compile(r"(\d{3,4}-\d{7})(\d{3,4}-\d{7})")
            pattern8 = re.compile(r"(\d{3,4}-\d{8})(\d{11})(\d{11})")
            pattern9 = re.compile(r"(\d{3,4}-\d{7})(\d{7})")
            if re.match(pattern1, data["TEL_"]):
                phone_number = re.sub(pattern1, r"\1  \2", data["TEL_"])
            elif re.match(pattern2, data["TEL_"]):
                phone_number = re.sub(pattern2, r"\1  \2", data["TEL_"])
            elif re.match(pattern3, data["TEL_"]):
                phone_number = re.sub(pattern3, r"\1  \2", data["TEL_"])
            elif re.match(pattern4, data["TEL_"]):
                phone_number = re.sub(pattern4, r"\1  \2  \3", data["TEL_"])
            elif re.match(pattern5, data["TEL_"]):
                phone_number = re.sub(pattern5, r"\1  \2", data["TEL_"])
            elif re.match(pattern6, data["TEL_"]):
                phone_number = re.sub(pattern6, r"\1  \2", data["TEL_"])
            elif re.match(pattern7, data["TEL_"]):
                phone_number = re.sub(pattern7, r"\1  \2", data["TEL_"])
            elif re.match(pattern8, data["TEL_"]):
                phone_number = re.sub(pattern8, r"\1  \2  \3", data["TEL_"])
            elif re.match(pattern9, data["TEL_"]):
                phone_number = re.sub(pattern9, r"\1  \2", data["TEL_"])
            else:
                phone_number = data["TEL_"]
            re_data["TEL_"] = phone_number
        # 地址
        if "ADDR_" in data:
            re_data["ADDR_"] = data["ADDR_"]
        re_data = super(Branchssxx, self).generic_shuffle(data=data, re_data=re_data, field=None)
        return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]


if __name__ == '__main__':
    param = sys.argv[1]
    # param = "{'limit_Number': '1000'}"
    verify_field = {'URL_': 'URL_'}
    script = Branchssxx(table_name=TABLE_NAME("CHA_BRANCH_SCHOOL"), collection_name="WD_SS_XX", param=param, verify_field=verify_field)
    script.main()
    script.close_client()