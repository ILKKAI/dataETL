# -*- coding: utf-8 -*-
"""华夏银行网点 HXBORGANIZE  有经纬度"""
from branch_scripts import GenericScript
from database._mongodb import MongoClient


def data_shuffle(data, province_list, city_list, area_list):
    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    re_data = dict()
    prov_n = None
    prov_c = None
    city_n = None
    city_c = None
    area_n = None
    area_c = None
    addr_ = None

    # 省级信息清洗
    for prov in province_list:
        if prov["NAME_"][:2] == data["PROVINCE_NAME_"][:2]:
            prov_n = prov["NAME_"]
            prov_c = prov["CODE_"]
            break
    if prov_c[:2] != data["CITY_CODE_"][:2]:
        data["CITY_CODE_"] = ""

    # 市级信息清洗
    for area in area_list:
        if area["CODE_"] == data["CITY_CODE_"]:
            area_n = area["NAME_"]
            area_c = area["CODE_"]
            city_c = area["CODE_"][:-2] + "00"
    for city in city_list:
        if city["CODE_"] == data["CITY_CODE_"]:
            city_n = city["NAME_"]
            city_c = city["CODE_"]
        elif city["CODE_"] == city_c:
            city_n = city["NAME_"]
    for prov in province_list:
        if prov["CODE_"] == data["CITY_CODE_"]:
            city_n = prov["NAME_"]
            city_c = prov["CODE_"]
            # print(data["ADDR_"], data["PROVINCE_NAME_"], prov_n)
        elif prov["NAME_"] in data["ADDR_"][:len(prov["NAME_"])]:
            prov_n = prov["NAME_"]
            prov_c = prov["CODE_"]
    if not city_c:
        for city in city_list:
            if city["NAME_"] in data["ADDR_"][:len(city["NAME_"])+4]:
                city_c = city["CODE_"]
                city_n = city["NAME_"]
                if city_c[:2] != prov_c[:2]:
                    prov_c = city_c[:2] + "00"
                for prov in province_list:
                    if prov_c == prov["NAME_"]:
                        prov_n = prov["NAME_"]
                        break
    if not city_c:
        if prov_n == "天津市":
            city_n = "天津市"
            city_c = "120100"
        elif data["NAME_"] == "新塘支行":
            city_n = "广州市"
            city_c = "440100"
            data["ADDR_"] = data["ADDR_"].replace("广州", "广州市")
        elif data["ADDR_"] == "天津市宝坻区新都汇广场1-1-105":
            prov_n = "天津市"
            prov_c = "1200"
            city_n = "天津市"
            city_c = "120100"

    # 区级县级信息清洗
    for area in area_list:
        if area["CODE_"][:2] == prov_c[:2]:
            if area["NAME_"] in data["ADDR_"]:
                area_n = area["NAME_"]
                area_c = area["CODE_"]
    # 其余无法匹配到区县级
    # if not area_c:
    #     print(prov_c, prov_n, city_c, city_n, data["ADDR_"])

    # 特殊情况
    if data["NAME_"] == "西咸新区分行营业部":
        data["LNG_"] = "108.73137"
        data["LAT_"] = "34.322323"
        prov_c = "6100"
        prov_n = "陕西省"
        city_c = "610400"
        city_n = "咸阳市"
        area_c = "610402"
        area_n = "秦都区"

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

    # # 添加分行编码
    # branch_code = None
    # for i in range(1, 10000):
    #     branch_code = "HXB" + "_" + city_c + "_" + "00000"
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
    re_data["BANK_CODE_"] = "HXB"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    re_data["AREA_CODE_"] = area_c
    re_data["UNIT_CODE_"] = "HXB" + "_" + city_c

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
    re_data["BUSINESS_HOURS_"] = data["BUSINESS_HOURS_"]

    # "S"
    re_data["STATUS_1"] = "1"

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="HXBORGANIZE", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
