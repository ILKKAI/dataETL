# -*- coding: utf-8 -*-
"""平安银行 ATM PABORGANIZE1"""
# TODO if all the data have province name and city name
from branch_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()

    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    prov_c = ""
    prov_n = ""
    city_c = ""
    city_n = ""
    addr_ = data["ADDR_"]

    # 省市级信息
    if "北京" in data["CITY_NAME_"]:
        prov_n = "北京市"
        prov_c = "1100"
        city_n = "北京市"
        city_c = "110100"
    elif "天津" in data["CITY_NAME_"]:
        prov_n = "天津市"
        prov_c = "1200"
        city_n = "天津市"
        city_c = "120100"
    elif "上海" in data["CITY_NAME_"]:
        prov_n = "上海市"
        prov_c = "3100"
        city_n = "上海市"
        city_c = "310100"
    elif "重庆" in data["CITY_NAME_"]:
        prov_n = "重庆市"
        prov_c = "5000"
        city_n = "重庆市"
        city_c = "500100"
    else:
        for city in city_list:
            if city["NAME_"][:-1] in data["CITY_NAME_"]:
                city_n = city["NAME_"]
                city_c = city["CODE_"]
                prov_c = city["PARENT_"]
                break
        if prov_c:
            for prov in province_list:
                if prov["CODE_"] == prov_c:
                    prov_n = prov["NAME_"]
                    break

    # 地址清洗
    if prov_n in addr_:
        pass
    elif prov_n[:-1] in addr_[:len(prov_n)]:
        addr_ = addr_[:len(prov_n)].replace(prov_n[:-1], prov_n) + addr_[len(prov_n):]
    elif prov_n[:4] in addr_[:len(prov_n)]:
        addr_ = addr_[:len(prov_n)].replace(prov_n[:4], prov_n) + addr_[len(prov_n):]
    elif prov_n[:3] in addr_[:len(prov_n)]:
        addr_ = addr_[:len(prov_n)].replace(prov_n[:3], prov_n) + addr_[len(prov_n):]
    elif prov_n[:2] in addr_[:len(prov_n)]:
        addr_ = addr_[:len(prov_n)].replace(prov_n[:2], prov_n) + addr_[len(prov_n):]
    else:
        addr_ = prov_n + addr_

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

    # "C"
    re_data["BANK_CODE_"] = "PAB"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-4]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    # re_data["AREA_CODE_"] = area_c
    # re_data["AREA_NAME_"] = area_n
    # re_data["UNIT_CODE_"] = "CZB" + "_" + city_c

    # "F"
    re_data["ADDR_"] = addr_
    # re_data["CITY_CODE_"] = city_c
    # re_data["CITY_NAME_"] = city_n
    # re_data["LAT_"] = lat
    # re_data["LNG_"] = lng
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
    re_data["UNIT_CODE_"] = "PAB" + "_" + re_data.get("CITY_CODE_", "")

    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["URL_"] = data["URL_"]
    # re_data["TEL_"] = tel
    # re_data["BUSINESS_HOURS_"] = business_time
    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
    re_data["TYPE_NAME_"] = "自助银行"
    re_data["TYPE_"] = "ZZ"

    # # 添加分行编码
    # branch_code = None
    # for i in range(1, 10000):
    #     branch_code = "PAB" + "_" + city_c + "_" + "00000"
    #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
    #     if branch_code in branch_code_list:
    #         continue
    #     else:
    #         branch_code_list.append(branch_code)
    #         break

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="PABORGANIZE1", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
