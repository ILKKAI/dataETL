# -*- coding: utf-8 -*-
"""广发银行网点 CGBORGANIZE"""
from branch_scripts2 import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()

    for each in ["北京市", "天津市", "上海市", "重庆市"]:
        if each in data["CITY_NAME_"]:
            for pro in province_list:
                if pro["NAME_"] == each:
                    re_data["PROVINCE_NAME_"] = pro["NAME_"]
                    re_data["PROVINCE_CODE_"] = pro["CODE_"]
                    re_data["CITY_NAME_"] = pro["NAME_"]
                    re_data["CITY_CODE_"] = pro["CODE_"][:3] + "100"
                    break
    else:
        for city in city_list:
            if city["NAME_"] in data["CITY_NAME_"]:
                re_data["CITY_NAME_"] = city["NAME_"]
                re_data["CITY_CODE_"] = city["CODE_"]
                re_data["PROVINCE_CODE_"] = city["PARENT_"]
                break
        if re_data.get("PROVINCE_CODE_"):
            for pro in province_list:
                if pro["CODE_"] == re_data["PROVINCE_CODE_"]:
                    re_data["PROVINCE_NAME_"] = pro["NAME_"]
                    break

    # # 添加分行编码
    # branch_code = None
    # for i in range(1, 10000):
    #     branch_code = "CGB" + "_" + city_c + "_" + "00000"
    #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
    #     if branch_code in branch_code_list:
    #         continue
    #     else:
    #         branch_code_list.append(branch_code)
    #         break

    # "C"
    re_data["BANK_CODE_"] = "CGB"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-3]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]

    # "F"
    # "地址：宝山区牡丹江路１２１１号"
    re_data["ADDR_"] = data["ADDR_"].replace("地址：", "")
    re_data["NAME_"] = data["NAME_"]

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
    re_data["UNIT_CODE_"] = "CGB" + "_" + re_data.get("CITY_CODE_", "")

    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["URL_"] = data["URL_"]

    re_data["URL_"] = data["URL_"]
    if "TEL_" in data:
        # 电话：02168037370
        re_data["TEL_"] = data["TEL_"].replace("电话：", "")
    if "BUSINESS_HOURS_" in data:
        re_data["BUSINESS_HOURS_"] = data["BUSINESS_HOURS_"]
    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
    # if ("社区银行" in re_data.get("SOURCE_TYPE_NAME_", "")) or ("网点" in re_data.get("SOURCE_TYPE_NAME_", "")):
    #     re_data["TYPE_NAME_"] = "支行"
    #     re_data["TYPE_"] = "ZH"
    # else:
    #     re_data["TYPE_NAME_"] = "自助银行"
    #     re_data["TYPE_"] = "ZZ"

    re_data["TYPE_NAME_"] = "支行"
    re_data["TYPE_"] = "ZH"

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="CGBORGANIZE", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
