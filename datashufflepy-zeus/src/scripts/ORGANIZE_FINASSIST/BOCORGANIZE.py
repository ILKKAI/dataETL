# -*- coding: utf-8 -*-

# 中国银行网点 BOCORGANIZE  无经纬度
# 已处理 15255

import hashlib
import re
from scripts import GenericScript


def data_shuffle(data, province_list, city_list, area_list):
    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    if "AREA_NAME_" not in data:
        return None
        # continue
    else:
        re_data = dict()
        prov_n = ""
        prov_c = ""
        city_n = ""
        city_c = ""
        area_n = ""
        area_c = ""
        addr_ = ""

        # 内蒙古, 广西, 新疆, 宁夏, 西藏 字段统一：
        if ("内蒙古" in data["ADDR_"][:3] or "广西" in data["ADDR_"][:2] or "新疆" in data["ADDR_"][:2] or
                "宁夏" in data["ADDR_"][:2] or "西藏" in data["ADDR_"][:2]):
            if "自治区" not in data["ADDR_"]:
                data["ADDR_"] = data["ADDR_"].replace("内蒙古", "内蒙古自治区")
                data["ADDR_"] = data["ADDR_"].replace("广西", "广西壮族自治区")
                data["ADDR_"] = data["ADDR_"].replace("新疆", "新疆维吾尔自治区")
                data["ADDR_"] = data["ADDR_"].replace("宁夏", "宁夏回族自治区")
                data["ADDR_"] = data["ADDR_"].replace("西藏", "西藏自治区")

        elif "京山县" in data["AREA_NAME_"]:
            data["AREA_NAME_"] = data["AREA_NAME_"].replace("荆州", "荆门")

        for city in city_list:
            if city["NAME_"] in data["AREA_NAME_"]:
                city_n = city["NAME_"]
                city_c = city["CODE_"]
                prov_c = city["CODE_"][:2] + "00"
                break
        for area in area_list:
            if city_c:
                if area["PARENT_"] == city_c:
                    if area["NAME_"] in data["AREA_NAME_"]:
                        area_n = area["NAME_"]
                        area_c = area["CODE_"]
                        break
            else:
                if (area["NAME_"][-1] == "区") and (len(area["NAME_"]) == 2):
                    continue
                if area["NAME_"] in data["AREA_NAME_"]:
                    area_n = area["NAME_"]
                    area_c = area["CODE_"]
                    city_c = area["CODE_"][:-2] + "00"
                    prov_c = area["CODE_"][:2] + "00"
                    break
        for prov in province_list:
            if prov_c:
                if prov["CODE_"] == prov_c:
                    prov_n = prov["NAME_"]
                    prov_c = prov["CODE_"]
                    break
            else:
                if prov["NAME_"] in data["AREA_NAME_"]:
                    prov_n = prov["NAME_"]
                    prov_c = prov["CODE_"]
                    break
                elif prov["NAME_"][:-1] in data["AREA_NAME_"]:
                    prov_n = prov["NAME_"]
                    prov_c = prov["CODE_"]
                    break

        if data["AREA_NAME_"] == "洋浦经济开发区":
            prov_n = "海南省"
            prov_c = "4600"
            city_n = "儋州市"
            city_c = "460400"
            area_n = "洋浦经济开发区"
            area_c = ""
        elif ("北京" in data["AREA_NAME_"][:3] or "天津" in data["AREA_NAME_"][:3] or
              "上海" in data["AREA_NAME_"][:3] or "重庆" in data["AREA_NAME_"][:3]):
            city_n = prov_n
            city_c = prov_c

        if not area_c:
            for area in area_list:
                if area["PARENT_"] == city_c:
                    if area["NAME_"][:2] in data["AREA_NAME_"][-len(area["NAME_"]):]:
                        area_n = area["NAME_"]
                        area_c = area["CODE_"]
                        break
        if not area_c:
            for area in area_list:
                if area["PARENT_"] == city_c:
                    if area["NAME_"] in data["ADDR_"]:
                        area_n = area["NAME_"]
                        area_c = area["CODE_"]

        # 地址清洗
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

        # 去除地址尾部邮编
        addr_ = re.sub(r"[(（][0-9]{5,6}[)）]|[(（][)）]", "", addr_)

        # # 添加分行编码
        # branch_code = None
        # for i in range(1, 10000):
        #     branch_code = "BOC" + "_" + city_c + "_" + "00000"
        #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
        #     if branch_code in branch_code_list:
        #         continue
        #     else:
        #         branch_code_list.append(branch_code)
        #         break

        # "C"
        hash_m = hashlib.md5()
        hash_m.update(data["NAME_"].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        re_data["ID_"] = (data["ENTITY_CODE_"] + "_" +
                          str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(hash_title))
        re_data["BANK_CODE_"] = "BOC"
        re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        re_data["AREA_CODE_"] = area_c
        re_data["UNIT_CODE_"] = "BOC" + "_" + city_c

        # "F"
        re_data["ADDR_"] = addr_
        re_data["CITY_CODE_"] = city_c
        re_data["CITY_"] = city_n
        re_data["LAT_"] = ""
        re_data["LNG_"] = ""
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
    script = GenericScript(entity_code="BOCORGANIZE", entity_type="ORGANIZE_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    for data in mongo_data_list:
        batch_list = data_shuffle(data, province_list, city_list, area_list)


if __name__ == '__main__':
    run()
