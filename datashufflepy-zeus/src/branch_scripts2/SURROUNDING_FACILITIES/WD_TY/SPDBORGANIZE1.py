# -*- coding: utf-8 -*-
"""上海浦东发展银行网站网点 SPDBORGANIZE1  有经纬度"""
from branch_scripts2 import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()
    dis_result = get_area(",".join([data["LAT_"], data["LNG_"]]))
    try:
        data["AREA_NAME_"] = dis_result["result"]["addressComponent"]["district"]
    except KeyError:
        data["AREA_NAME_"] = ""
    try:
        data["AREA_CODE_"] = dis_result["result"]["addressComponent"]["adcode"]
    except KeyError:
        data["AREA_CODE_"] = ""
    else:
        data["CITY_CODE_"] = data["AREA_CODE_"][:4] + "00"
        data["PROVINCE_CODE_"] = data["AREA_CODE_"][:2] + "00"
        for city in city_list:
            if city["CODE_"] == data["CITY_CODE_"]:
                data["CITY_NAME_"] = city["NAME_"]
                break
        for prov in province_list:
            if prov["CODE_"] == data["PROVINCE_CODE_"]:
                data["PROVINCE_NAME_"] = prov["NAME_"]
                break

    if not data["AREA_NAME_"]:
        for prov in province_list:
            if prov["NAME_"] in data["ADDR_"]:
                data["PROVINCE_NAME_"] = prov["NAME_"]
                data["PROVINCE_CODE_"] = prov["CODE_"]
                break
            elif prov["NAME_"][:-1] in data["ADDR_"]:
                data["PROVINCE_NAME_"] = prov["NAME_"]
                data["PROVINCE_CODE_"] = prov["CODE_"]
                break
            elif prov["NAME_"][:2] in data["ADDR_"]:
                data["PROVINCE_NAME_"] = prov["NAME_"]
                data["PROVINCE_CODE_"] = prov["CODE_"]
                break

        for city in city_list:
            if city["NAME_"] in data["ADDR_"]:
                data["CITY_NAME_"] = city["NAME_"]
                data["CITY_CODE_"] = city["CODE_"]
                break
            elif city["NAME_"][:-1] in data["ADDR_"]:
                data["CITY_NAME_"] = city["NAME_"]
                data["CITY_CODE_"] = city["CODE_"]
                break
            elif city["NAME_"][:2] in data["ADDR_"]:
                data["CITY_NAME_"] = city["NAME_"]
                data["CITY_CODE_"] = city["CODE_"]
                break

        if data["CITY_CODE_"] and not data["PROVINCE_CODE_"]:
            data["PROVINCE_CODE_"] = data["CITY_CODE_"][:2] + "00"
            for prov in province_list:
                if prov["CODE_"] == data["PROVINCE_CODE_"]:
                    data["PROVINCE_NAME_"] = prov["NAME_"]

        if data.get("CITY_CODE_"):
            for area in area_list:
                if area["PARENT_"] == data["CITY_CODE_"]:
                    if area["NAME_"] in data["ADDR_"]:
                        data["AREA_NAME_"] = area["NAME_"]
                        data["AREA_CODE_"] = area["CODE_"]
                        break
                    elif area["NAME_"][:-1] in data["ADDR_"]:
                        data["AREA_NAME_"] = area["NAME_"]
                        data["AREA_CODE_"] = area["CODE_"]
                        break
                    elif area["NAME_"][:2] in data["ADDR_"]:
                        data["AREA_NAME_"] = area["NAME_"]
                        data["AREA_CODE_"] = area["CODE_"]
                        break

    if data.get("PROVINCE_NAME_", "no value") in ["北京市", "天津市", "上海市", "重庆市"]:
        data["CITY_NAME_"] = data["PROVINCE_NAME_"]
        data["CITY_CODE_"] = data["PROVINCE_CODE_"]

    prov_n = data.get("PROVINCE_NAME_", "")
    city_n = data.get("CITY_NAME_", "")

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
    re_data["BANK_CODE_"] = "SPDB"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-5]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]

    re_data["ADDR_"] = addr_
    # re_data["CITY_CODE_"] = city_c
    # re_data["CITY_"] = city_n
    # re_data["PROVINCE_CODE_"] = prov_c
    # re_data["PROVINCE_NAME_"] = prov_n
    # re_data["DISTRICT_CODE_"] = area_c
    # re_data["DISTRICT_NAME_"] = area_n
    # re_data["LAT_"] = data["LAT_"]
    # re_data["LNG_"] = data["LNG_"]

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

    re_data["UNIT_CODE_"] = "SPDB" + "_" + re_data.get("CITY_CODE_", "")
    re_data["NAME_"] = data["NAME_"]
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["URL_"] = data["URL_"]
    # re_data["TEL_"] = ""
    # re_data["BUSINESS_HOURS_"] = ""
    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
    re_data["TYPE_NAME_"] = "自助银行"
    re_data["TYPE_"] = "ZZ"
    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="SPDBORGANIZE1", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
