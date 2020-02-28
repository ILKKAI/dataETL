# -*- coding: utf-8 -*-
"""中国银行 ATM BOCORGANIZE1  无经纬度"""
from branch_scripts2 import GenericScript
from database._mongodb import MongoClient


# TODO if all of the data's addr have no province name and city name
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    re_data = dict()

    # for prov in province_list:
    #     if prov["NAME_"][:2] in data["PROVINCE_NAME_"]:
    #         re_data["PROVINCE_CODE_"] = prov["CODE_"]
    #         re_data["PROVINCE_NAME_"] = prov["NAME_"]
    #         break
    # for city in city_list:
    #     if city["NAME_"][:2] in data["CITY_NAME_"]:
    #         re_data["CITY_CODE_"] = city["CODE_"]
    #         re_data["CITY_NAME_"] = city["NAME_"]
    #         break

    # prov_n = ""
    # prov_c = ""
    # city_n = ""
    # city_c = ""
    # area_n = ""
    # area_c = ""
    # addr_ = ""
    #
    # # 内蒙古, 广西, 新疆, 宁夏, 西藏 字段统一：
    # if ("内蒙古" in data["ADDR_"][:3] or "广西" in data["ADDR_"][:2] or "新疆" in data["ADDR_"][:2] or
    #         "宁夏" in data["ADDR_"][:2] or "西藏" in data["ADDR_"][:2]):
    #     if "自治区" not in data["ADDR_"]:
    #         data["ADDR_"] = data["ADDR_"].replace("内蒙古", "内蒙古自治区")
    #         data["ADDR_"] = data["ADDR_"].replace("广西", "广西壮族自治区")
    #         data["ADDR_"] = data["ADDR_"].replace("新疆", "新疆维吾尔自治区")
    #         data["ADDR_"] = data["ADDR_"].replace("宁夏", "宁夏回族自治区")
    #         data["ADDR_"] = data["ADDR_"].replace("西藏", "西藏自治区")
    #
    # elif "京山县" in data["AREA_NAME_"]:
    #     data["AREA_NAME_"] = data["AREA_NAME_"].replace("荆州", "荆门")
    #
    # for city in city_list:
    #     if city["NAME_"] in data["AREA_NAME_"]:
    #         city_n = city["NAME_"]
    #         city_c = city["CODE_"]
    #         prov_c = city["CODE_"][:2] + "00"
    #         break
    # for area in area_list:
    #     if city_c:
    #         if area["PARENT_"] == city_c:
    #             if area["NAME_"] in data["AREA_NAME_"]:
    #                 area_n = area["NAME_"]
    #                 area_c = area["CODE_"]
    #                 break
    #     else:
    #         if (area["NAME_"][-1] == "区") and (len(area["NAME_"]) == 2):
    #             continue
    #         if area["NAME_"] in data["AREA_NAME_"]:
    #             area_n = area["NAME_"]
    #             area_c = area["CODE_"]
    #             city_c = area["CODE_"][:-2] + "00"
    #             prov_c = area["CODE_"][:2] + "00"
    #             break
    # for prov in province_list:
    #     if prov_c:
    #         if prov["CODE_"] == prov_c:
    #             prov_n = prov["NAME_"]
    #             prov_c = prov["CODE_"]
    #             break
    #     else:
    #         if prov["NAME_"] in data["AREA_NAME_"]:
    #             prov_n = prov["NAME_"]
    #             prov_c = prov["CODE_"]
    #             break
    #         elif prov["NAME_"][:-1] in data["AREA_NAME_"]:
    #             prov_n = prov["NAME_"]
    #             prov_c = prov["CODE_"]
    #             break
    #
    # if data["AREA_NAME_"] == "洋浦经济开发区":
    #     prov_n = "海南省"
    #     prov_c = "4600"
    #     city_n = "儋州市"
    #     city_c = "460400"
    #     area_n = "洋浦经济开发区"
    #     area_c = ""
    # elif ("北京" in data["AREA_NAME_"][:3] or "天津" in data["AREA_NAME_"][:3] or
    #       "上海" in data["AREA_NAME_"][:3] or "重庆" in data["AREA_NAME_"][:3]):
    #     city_n = prov_n
    #     city_c = prov_c
    #
    # if not area_c:
    #     for area in area_list:
    #         if area["PARENT_"] == city_c:
    #             if area["NAME_"][:2] in data["AREA_NAME_"][-len(area["NAME_"]):]:
    #                 area_n = area["NAME_"]
    #                 area_c = area["CODE_"]
    #                 break
    # if not area_c:
    #     for area in area_list:
    #         if area["PARENT_"] == city_c:
    #             if area["NAME_"] in data["ADDR_"]:
    #                 area_n = area["NAME_"]
    #                 area_c = area["CODE_"]
    #
    # # 地址清洗
    # if prov_n in data["ADDR_"]:
    #     addr_ = data["ADDR_"]
    # elif prov_n[:-1] in data["ADDR_"][:len(prov_n)]:
    #     addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:-1], prov_n) + data["ADDR_"][len(prov_n):]
    # elif prov_n[:4] in data["ADDR_"][:len(prov_n)]:
    #     addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:4], prov_n) + data["ADDR_"][len(prov_n):]
    # elif prov_n[:3] in data["ADDR_"][:len(prov_n)]:
    #     addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:3], prov_n) + data["ADDR_"][len(prov_n):]
    # elif prov_n[:2] in data["ADDR_"][:len(prov_n)]:
    #     addr_ = data["ADDR_"][:len(prov_n)].replace(prov_n[:2], prov_n) + data["ADDR_"][len(prov_n):]
    # else:
    #     addr_ = prov_n + data["ADDR_"]
    #
    # if city_n in addr_[:len(prov_n) + len(city_n)]:
    #     addr_ = addr_
    # elif city_n[:-1] in addr_[:len(prov_n) + len(city_n)]:
    #     addr_ = addr_[:len(prov_n) + len(city_n)].replace(
    #         city_n[:-1], city_n) + addr_[len(prov_n) + len(city_n):]
    # elif city_n[:4] in addr_[:len(prov_n) + len(city_n)]:
    #     addr_ = addr_[:len(prov_n) + len(city_n)].replace(
    #         city_n[:4], city_n) + addr_[len(prov_n) + len(city_n):]
    # elif city_n[:3] in addr_[:len(prov_n) + len(city_n)]:
    #     addr_ = addr_[:len(prov_n) + len(city_n)].replace(
    #         city_n[:3], city_n) + addr_[len(prov_n) + len(city_n):]
    # elif city_n[:2] in addr_[:len(prov_n) + len(city_n)]:
    #     addr_ = addr_[:len(prov_n) + len(city_n)].replace(
    #         city_n[:2], city_n) + addr_[len(prov_n) + len(city_n):]
    # else:
    #     addr_ = addr_[:len(prov_n)] + city_n + addr_[len(prov_n):]
    #
    # # # 添加分行编码
    # # branch_code = None
    # # for i in range(1, 10000):
    # #     branch_code = "BOC" + "_" + city_c + "_" + "00000"
    # #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
    # #     if branch_code in branch_code_list:
    # #         continue
    # #     else:
    # #         branch_code_list.append(branch_code)
    # #         break

    # "C"
    re_data["BANK_CODE_"] = "BOC"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-7]
    # re_data["AREA_CODE_"] = area_c
    # re_data["AREA_NAME_"] = area_n
    # re_data["UNIT_CODE_"] = "BOC" + "_" + re_data["CITY_CODE_"]

    # "F"
    re_data["ADDR_"] = data["PROVINCE_NAME_"] + data["CITY_NAME_"] + data['ADDR_']
    # re_data["CITY_CODE_"] = city_c
    # re_data["CITY_NAME_"] = city_n
    # re_data["LAT_"] = ""
    # re_data["LNG_"] = ""
    re_data["NAME_"] = re_data["ADDR_"] + data["NAME_"]
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
    re_data["UNIT_CODE_"] = "BOC" + "_" + re_data.get("CITY_CODE_", "")

    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    re_data["URL_"] = data["URL_"]
    if "TEL_" in data:
        re_data["TEL_"] = data["TEL_"]
    if "BUSINESS_HOURS_" in data:
        re_data["BUSINESS_HOURS_"] = data["BUSINESS_HOURS_"]
    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
    re_data["TYPE_NAME_"] = "自助银行"
    re_data["TYPE_"] = "ZZ"

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="BOCORGANIZE1", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
