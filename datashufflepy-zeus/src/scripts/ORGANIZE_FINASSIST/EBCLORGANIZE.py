# -*- coding: utf-8 -*-

# 恒丰银行网点 EBCLORGANIZE  有经纬度
import hashlib

from scripts import GenericScript


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()
    prov_n = ""
    prov_c = ""
    city_n = ""
    city_c = ""
    area_n = ""
    area_c = ""
    addr_ = ""

    for prov in province_list:
        if prov["NAME_"] in data["ADDR_"]:
            prov_n = prov["NAME_"]
            prov_c = prov["CODE_"]

    for city in city_list:
        if city["NAME_"] in data["ADDR_"]:
            city_n = city["NAME_"]
            city_c = city["CODE_"]
            if not prov_c:
                prov_c = city["PARENT_"]

    if prov_c and (not prov_n):
        for prov in province_list:
            if prov_c == prov["CODE_"]:
                prov_n = prov["NAME_"]

    for area in area_list:
        if city_c:
            if area["PARENT_"] == city_c:
                if area["NAME_"] in data["ADDR_"]:
                    area_n = area["NAME_"]
                    area_c = area["CODE_"]
    if city_n and prov_n:
        # 地址清洗
        if prov_n and city_n:
            if prov_n in data["ADDR_"][:len(prov_n)]:
                if city_n in data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)]:
                    addr_ = data["ADDR_"]
                elif city_n[:2] in data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)]:
                    addr_ = (data["ADDR_"][:len(prov_n)] + data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)].
                             replace(city_n[:2], city_n) + data["ADDR_"][len(prov_n)+len(city_n):])
                else:
                    addr_ = data["ADDR_"][:len(prov_n)] + city_n + data["ADDR_"][len(prov_n)+len(city_n):]

            elif prov_n[:2] in data["ADDR_"][:len(prov_n)]:
                data["ADDR_"] = data["ADDR_"][:len(prov_n)].replace(prov_n[:2], prov_n)
                if city_n in data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)]:
                    addr_ = data["ADDR_"]
                elif city_n[:2] in data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)]:
                    addr_ = (data["ADDR_"][:len(prov_n)] + data["ADDR_"][len(prov_n):len(prov_n) + len(city_n)].
                             replace(city_n[:2], city_n) + data["ADDR_"][len(prov_n)+len(city_n):])
                else:
                    addr_ = data["ADDR_"][:len(prov_n)] + city_n + data["ADDR_"][len(prov_n)+len(city_n):]

            else:
                data["ADDR_"] = prov_n + data["ADDR_"]
                if city_n in data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)]:
                    addr_ = data["ADDR_"]
                elif city_n[:2] in data["ADDR_"][len(prov_n):len(prov_n)+len(city_n)]:
                    addr_ = (data["ADDR_"][:len(prov_n)] + data["ADDR_"][len(prov_n):len(prov_n) + len(city_n)].
                             replace(city_n[:2], city_n) + data["ADDR_"][len(prov_n)+len(city_n):])
                else:
                    addr_ = data["ADDR_"][:len(prov_n)] + city_n + data["ADDR_"][len(prov_n)+len(city_n):]
    else:
        addr_ = data["ADDR_"]

    # "C"
    hash_m = hashlib.md5()
    hash_m.update(data["NAME_"].encode("utf-8"))
    hash_title = hash_m.hexdigest()
    re_data["ID_"] = (data["ENTITY_CODE_"] + "_" +
                      str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(hash_title))
    re_data["BANK_CODE_"] = "EBCL"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    re_data["AREA_CODE_"] = area_c
    re_data["UNIT_CODE_"] = "EBCL" + "_" + city_c

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
    re_data["TEL_"] = data["TEL_"]
    re_data["BUSINESS_HOURS_"] = ""

    # "S"
    re_data["STATUS_1"] = "1"

    return re_data


def run():
    script = GenericScript(entity_code="EBCLORGANIZE", entity_type="ORGANIZE_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    for data in mongo_data_list:
        batch_list = data_shuffle(data, province_list, city_list, area_list)


if __name__ == '__main__':
    run()
