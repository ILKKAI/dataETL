# -*- coding: utf-8 -*-

# 上海浦东发展银行网站网点 SPDBORGANIZE  有经纬度

# 使用高德API接口
import hashlib

from scripts import GenericScript
from tools.web_api_of_baidu import get_lat_lng
from tools.web_api_of_gaode import gaode_get_lat_lng


def data_shuffle(data, province_list, city_list, area_list):
    if ("CITY_" not in data) or ("PROVINCE_NAME_" not in data):
        return None
    else:
        re_data = dict()
        prov_n = ""
        prov_c = ""
        city_n = ""
        city_c = ""
        area_n = ""
        area_c = ""
        addr_ = ""

        for prov in province_list:
            if prov["NAME_"] == data["PROVINCE_NAME_"]:
                prov_n = prov["NAME_"]
                prov_c = prov["CODE_"]

        if prov_n in ["北京市", "天津市", "上海市", "重庆市"]:
            city_n = prov_n
            city_c = prov_c

        if not city_c:
            for city in city_list:
                if city["NAME_"] == data["CITY_"]:
                    city_n = city["NAME_"]
                    city_c = city["CODE_"]
                    break

        for area in area_list:
            if area["PARENT_"] == city_c:
                if area["NAME_"] in data["ADDR_"]:
                    area_n = area["NAME_"]
                    area_c = area["CODE_"]

        # 地址清洗
        if ("广西自治区" in data["ADDR_"]) or ("新疆自治区" in data["ADDR_"]):
            data["ADDR_"] = data["ADDR_"].replace("广西自治区", "广西壮族自治区")
            data["ADDR_"] = data["ADDR_"].replace("新疆自治区", "新疆维吾尔自治区")
        if prov_n in data["ADDR_"]:
            addr_ = data["ADDR_"]
        elif prov_n[:-1] in data["ADDR_"][:len(prov_n)]:
            addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:-1], prov_n) + data["ADDR_"][len(prov_n):]
        elif prov_n[:4] in data["ADDR_"][:len(prov_n)]:
            addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:4], prov_n) + data["ADDR_"][len(prov_n):]
        elif prov_n[:3] in data["ADDR_"][:len(prov_n)]:
            addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:3], prov_n) + data["ADDR_"][len(prov_n):]
        elif prov_n[:2] in data["ADDR_"][:len(prov_n)]:
            addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:2], prov_n) + data["ADDR_"][len(prov_n):]
        else:
            addr_ = prov_n + data["ADDR_"]

        if city_n in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_
        elif city_n[:-1] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:-1], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:4] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:4], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:3] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:3], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:2] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:2], city_n) + addr_[len(prov_n) + len(city_n):]
        else:
            addr_ = addr_[:len(prov_n)] + city_n + addr_[len(prov_n):]

        # # 添加分行编码
        # branch_code = None
        # for i in range(1, 10000):
        #     branch_code = "SPDB" + "_" + city_c + "_" + "00000"
        #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
        #     if branch_code in branch_code_list:
        #         continue
        #     else:
        #         branch_code_list.append(branch_code)
        #         break

        # "C"
        hash_m = hashlib.md5()
        hash_m.update(data["NAME_"].encode("utf-8"))
        hash_name = hash_m.hexdigest()
        re_data["ID_"] = (data["ENTITY_CODE_"] + "_" +
                          str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(hash_name))
        re_data["BANK_CODE_"] = "SPDB"
        re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        re_data["AREA_CODE_"] = area_c
        re_data["UNIT_CODE_"] = "SPDB" + "_" + city_c

        # "F"
        re_data["ADDR_"] = addr_
        re_data["CITY_CODE_"] = city_c
        re_data["CITY_"] = city_n
        re_data["LAT_"] = data["LAT_"]
        re_data["LNG_"] = data["LNG_"]
        re_data["NAME_"] = data["NAME_"]
        re_data["PROVINCE_CODE_"] = prov_c
        re_data["PROVINCE_NAME_"] = prov_n
        re_data["DISTRICT_CODE_"] = area_c
        re_data["DISTRICT_NAME_"] = area_n
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["URL_"] = data["URL_"]
        re_data["TEL_"] = ""
        re_data["BUSINESS_HOURS_"] = ""
        # "S"
        re_data["STATUS_1"] = "1"
        return re_data


def run():
    script = GenericScript(entity_code="SPDBORGANIZE", entity_type="ORGANIZE_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    batch_list = data_shuffle(mongo_data_list, province_list, city_list, area_list)


if __name__ == '__main__':
    run()
