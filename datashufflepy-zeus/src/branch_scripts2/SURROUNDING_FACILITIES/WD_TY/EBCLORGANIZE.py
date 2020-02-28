# -*- coding: utf-8 -*-
"""恒丰银行网点 EBCLORGANIZE  有经纬度"""
from branch_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()
    for each in ["北京市", "天津市", "上海市", "重庆市"]:
        if each[:2] in data["CITY_NAME_"]:
            for pro in province_list:
                if pro["NAME_"] == each:
                    re_data["PROVINCE_NAME_"] = pro["NAME_"]
                    re_data["PROVINCE_CODE_"] = pro["CODE_"]
                    re_data["CITY_NAME_"] = pro["NAME_"]
                    re_data["CITY_CODE_"] = pro["CODE_"][:3] + "100"
                    break
    else:
        for city in city_list:
            if city["NAME_"][:2] in data["CITY_NAME_"]:
                re_data["CITY_NAME_"] = city["NAME_"]
                re_data["CITY_CODE_"] = city["CODE_"]
                re_data["PROVINCE_CODE_"] = city["PARENT_"]
                break
        if re_data.get("PROVINCE_CODE_"):
            for pro in province_list:
                if pro["CODE_"] == re_data["PROVINCE_CODE_"]:
                    re_data["PROVINCE_NAME_"] = pro["NAME_"]
                    break

    city_n = re_data.get("CITY_NAME_", "")
    prov_n = re_data.get("PROVINCE_NAME_", "")
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
    re_data["BANK_CODE_"] = "EBCL"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-3]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    # re_data["AREA_CODE_"] = area_c
    # re_data["AREA_NAME_"] = area_n
    # re_data["UNIT_CODE_"] = "EBCL" + "_" + city_c

    # "F"
    if "addr_" not in dir():
        re_data["ADDR_"] = data["ADDR_"]
    else:
        re_data["ADDR_"] = addr_

    # re_data["CITY_CODE_"] = city_c
    # re_data["CITY_NAME_"] = city_n
    # re_data["LAT_"] = data["LAT_"]
    # re_data["LNG_"] = data["LNG_"]
    re_data["NAME_"] = data["NAME_"]
    # re_data["PROVINCE_CODE_"] = prov_c
    # re_data["PROVINCE_NAME_"] = prov_n

    result = get_lat_lng(address=re_data["ADDR_"])
    try:
        re_data["LAT_"] = str(result["result"]["location"]["lat"])
        re_data["LNG_"] = str(result["result"]["location"]["lng"])
    except KeyError:
        re_data["LAT_"] = ""
        re_data["LNG_"] = ""
    else:
        dis_result = get_area(",".join([re_data["LAT_"], re_data["LNG_"]]))
        try:
            re_data["AREA_NAME_"] = dis_result["result"]["addressComponent"]["district"]
        except KeyError:
            re_data["AREA_NAME_"] = ""
        try:
            re_data["AREA_CODE_"] = dis_result["result"]["addressComponent"]["adcode"]
        except KeyError:
            re_data["AREA_CODE_"] = ""
        else:
            re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
            re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
            for city in city_list:
                if city["CODE_"] == re_data["CITY_CODE_"]:
                    re_data["CITY_NAME_"] = city["NAME_"]
                    break
            for prov in province_list:
                if prov["CODE_"] == re_data["PROVINCE_CODE_"]:
                    re_data["PROVINCE_NAME_"] = prov["NAME_"]
                    break
    re_data["UNIT_CODE_"] = "EBCL" + "_" + re_data.get("CITY_CODE_", "")

    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["URL_"] = data["URL_"]
    if "TEL_" in data:
        re_data["TEL_"] = data["TEL_"]
    # re_data["BUSINESS_HOURS_"] = ""
    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
    re_data["TYPE_NAME_"] = "支行"
    re_data["TYPE_"] = "ZH"
    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="EBCLORGANIZE", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
