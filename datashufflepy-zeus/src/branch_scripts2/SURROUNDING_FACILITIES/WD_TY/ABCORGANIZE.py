# -*- coding: utf-8 -*-
"""中国农业银行网点 ABCORGANIZE"""
import re

from branch_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    re_data = dict()
    addr_ = None
    area_c = None
    area_n = None
    city_c = None
    city_n = None
    prov_c = None
    prov_n = None

    # 西藏地区编码与数据库编码不符，单独清理
    if "西藏" in data["PROVINCE_NAME_"]:
        data["CITY_NAME_"] = data["CITY_NAME_"].replace("西藏自治区", "")
        if "西藏" in data["ADDR_"]:
            data["ADDR_"] = data["ADDR_"].replace("西藏", "西藏自治区")
        else:
            data["ADDR_"] = "西藏自治区" + data["ADDR_"]

        for city in city_list:
            if city["CODE_"][:2] == "54":
                if data["CITY_NAME_"][:2] == city["NAME_"][:2]:
                    data["ADDR_"] = data["ADDR_"].replace(data["CITY_NAME_"], city["NAME_"])
                    data["CITY_NAME_"] = city["NAME_"]
                    data["CITY_CODE_"] = city["CODE_"]
                    data["ADDR_"] = data["ADDR_"].replace(data["CITY_NAME_"][:-1]+"地区", data["CITY_NAME_"])

                if data["CITY_NAME_"][:-1] not in data["ADDR_"]:
                    data["ADDR_"] = data["ADDR_"][:5] + data["CITY_NAME_"] + data["ADDR_"][5:]

    # 青海地区编码与数据库编码不符，单独清理
    if "青海" in data["PROVINCE_NAME_"]:
        data["PROVINCE_NAME_"] = "青海省"
        data["CITY_NAME_"] = data["CITY_NAME_"].replace("青海", "")

        if "青海省" not in data["ADDR_"]:
            data["ADDR_"] = "青海省" + data["ADDR_"]

        for city in city_list:
            if city["CODE_"][:2] == "63":
                if city["NAME_"][:2] == data["CITY_NAME_"][:2]:
                    data["CITY_NAME_"] = city["NAME_"]
                    data["CITY_CODE_"] = city["CODE_"]

            if data["CITY_NAME_"][:-1] not in data["ADDR_"]:
                data["ADDR_"] = data["ADDR_"][:3] + data["CITY_NAME_"] + data["ADDR_"][3:]

    # 新疆地区编码与数据库编码不符，单独清理
    if "新疆" in data["PROVINCE_NAME_"]:
        data["PROVINCE_NAME_"] = "新疆维吾尔自治区"
        data["CITY_NAME_"] = data["CITY_NAME_"].replace("新疆维吾尔自治区", "")
        data["CITY_NAME_"] = data["CITY_NAME_"].replace("新疆", "")

        if ("新疆维吾尔自治区" not in data["ADDR_"]) and ("新疆" not in data["ADDR_"]):
            data["ADDR_"] = "新疆维吾尔自治区" + data["ADDR_"]
        elif ("新疆" in data["ADDR_"]) and("新疆维吾尔自治区" not in data["ADDR_"]):
            data["ADDR_"] = "新疆维吾尔自治区" + data["ADDR_"][2:]

        for city in city_list:
            if city["CODE_"][:2] == "65":
                if city["NAME_"][:2] == data["CITY_NAME_"][:2]:
                    data["CITY_NAME_"] = city["NAME_"]
                    data["CITY_CODE_"] = city["CODE_"]

        # 哈密市只有一个伊州区，网点信息都是此区的
        if data["CITY_NAME_"] == "哈密市":
            data["AREA_NAME_"] = "伊州区"
            data["AREA_CODE_"] = "650502"
        for area in area_list:
            if area["CODE_"][:2] == "65":
                if area["NAME_"][:2] in data["AREA_NAME_"]:
                    data["AREA_NAME_"] = area["NAME_"]
                    data["AREA_CODE_"] = area["CODE_"]

    # 内蒙古, 广西, 宁夏 字段统一：
    if (("内蒙古" in data["ADDR_"]) or ("广西" in data["ADDR_"]) or
            ("新疆" in data["ADDR_"]) or ("宁夏" in data["ADDR_"])):
        if data["PROVINCE_NAME_"] not in data["ADDR_"]:
            data["ADDR_"] = data["ADDR_"].replace("内蒙古", "内蒙古自治区")
            data["ADDR_"] = data["ADDR_"].replace("广西", "广西壮族自治区")
            data["ADDR_"] = data["ADDR_"].replace("宁夏", "宁夏回族自治区")
        if data["PROVINCE_NAME_"] in data["CITY_NAME_"]:
            data["CITY_NAME_"] = data["CITY_NAME_"].replace(data["PROVINCE_NAME_"], "")
        if data["CITY_NAME_"][:-1] not in data["ADDR_"]:
            data["ADDR_"] = data["ADDR_"][:len(data["PROVINCE_NAME_"])] + data["CITY_NAME_"] + data["ADDR_"][len(data["PROVINCE_NAME_"]):]
            data["ADDR_"] = re.sub(r"{}{}地?区?市?".format(data["CITY_NAME_"], data["CITY_NAME_"][:2]), data["CITY_NAME_"], data["ADDR_"])
        if "区区" in data["ADDR_"]:
            data["ADDR_"] = data["ADDR_"].replace("区区", "区")

    # 吉林省吉林市清洗
    if "吉林" in data["PROVINCE_NAME_"]:
        if "吉林市" not in data["CITY_NAME_"]:
            data["CITY_NAME_"] = data["CITY_NAME_"].replace("吉林省", "")
            data["CITY_NAME_"] = data["CITY_NAME_"].replace("吉林", "")
            data["CITY_CODE_"] = "220200"

    # 省级名称清洗
    for prov in province_list:
        if prov["CODE_"][:2] == data["PROVINCE_CODE_"][:2]:
            data["PROVINCE_CODE_"] = prov["CODE_"]
            data["PROVINCE_NAME_"] = prov["NAME_"]
            break

    # 市级清洗
    if data["PROVINCE_NAME_"][:2] in data["CITY_NAME_"]:
        if data["CITY_NAME_"] == '北京市' or data["CITY_NAME_"] == '天津市' or data["CITY_NAME_"] == '上海市' or data["CITY_NAME_"] == '重庆市':
            pass
        else:
            data["CITY_NAME_"] = data["CITY_NAME_"].replace(data["PROVINCE_NAME_"], "")
            data["CITY_NAME_"] = data["CITY_NAME_"].replace(data["PROVINCE_NAME_"][:-1], "")
            data["CITY_NAME_"] = data["CITY_NAME_"].replace(data["PROVINCE_NAME_"][:3], "")
            data["CITY_NAME_"] = data["CITY_NAME_"].replace(data["PROVINCE_NAME_"][:2], "")
    for city in city_list:
        if city["NAME_"] == "市辖区":
            continue
        elif city["CODE_"][:2] == data["PROVINCE_CODE_"][:2]:
            if city["CODE_"] == data["CITY_CODE_"]:
                data["CITY_CODE_"] = city["CODE_"]
                data["CITY_NAME_"] = city["NAME_"]
                break
            elif (city["NAME_"][:2] == data["CITY_NAME_"][:2]) and (city["CODE_"] != data["CITY_CODE_"]):
                data["CITY_CODE_"] = city["CODE_"]
                data["CITY_NAME_"] = city["NAME_"]
                break
            elif (city["NAME_"] in data["ADDR_"][:len(data["PROVINCE_NAME_"])+len(city["NAME_"])]) and (not data["CITY_NAME_"]):
                data["CITY_CODE_"] = city["CODE_"]
                data["CITY_NAME_"] = city["NAME_"]
                break

    # 区县级清洗
    if data["PROVINCE_NAME_"][:2] in data["AREA_NAME_"]:
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["PROVINCE_NAME_"], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["PROVINCE_NAME_"][:-1], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["PROVINCE_NAME_"][:4], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["PROVINCE_NAME_"][:3], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["PROVINCE_NAME_"][:2], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["CITY_NAME_"], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["CITY_NAME_"][:-1], "")
        data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["CITY_NAME_"][:3], "")
        # data["AREA_NAME_"] = data["AREA_NAME_"].replace(data["CITY_NAME_"][:2], "")
        data["AREA_NAME_"] = data["AREA_NAME_"][:2].replace("地区", "") + data["AREA_NAME_"][2:]
    for area in area_list:
        if area["CODE_"][:2] == data["PROVINCE_CODE_"][:2]:
            if area["CODE_"] == data["AREA_CODE_"]:
                data["AREA_NAME_"] = area["NAME_"]
                data["AREA_CODE_"] = area["CODE_"]
                break
            elif (area["NAME_"] == data["AREA_NAME_"]) and (area["CODE_"] != data["AREA_CODE_"]):
                data["AREA_NAME_"] = area["NAME_"]
                data["AREA_CODE_"] = area["CODE_"]
                break
            elif ((area["NAME_"] in data["ADDR_"][:len(data["PROVINCE_NAME_"]) +
                  len(data["CITY_NAME_"]) + len(area["NAME_"])]) and (not data["AREA_NAME_"])):
                data["CITY_CODE_"] = city["CODE_"]
                data["CITY_NAME_"] = city["NAME_"]

    # 地址清洗
    # 地址中有省级和市级
    if (data["PROVINCE_NAME_"] in data["ADDR_"]) and (data["CITY_NAME_"] in data["ADDR_"]):
        addr_ = data["ADDR_"]

    # 地址中有省级没有市级
    elif (data["PROVINCE_NAME_"] in data["ADDR_"]) and (data["CITY_NAME_"] not in data["ADDR_"]):
        if data["CITY_NAME_"][:-1] in data["ADDR_"][:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])]:
            addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])] +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]):len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])].
                     replace(data["CITY_NAME_"][:-1], data["CITY_NAME_"]) +
                     data["ADDR_"][len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"]):])
        elif data["CITY_NAME_"][:3] in data["ADDR_"][:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])]:
            addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])] +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]):len(data["PROVINCE_NAME_"]) +
                     len(data["CITY_NAME_"])].replace(data["CITY_NAME_"][:3], data["CITY_NAME_"]) +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]):])
        elif data["CITY_NAME_"][:2] in data["ADDR_"][:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])]:
            addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])] +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]):len(data["PROVINCE_NAME_"]) +
                     len(data["CITY_NAME_"])].replace(data["CITY_NAME_"][:2], data["CITY_NAME_"]) +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]):])
        else:
            addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])] +
                     data["CITY_NAME_"] + data["ADDR_"][len(data["PROVINCE_NAME_"]):])

    # 地址中没有省级有市级
    elif (data["PROVINCE_NAME_"] not in data["ADDR_"]) and (data["CITY_NAME_"] in data["ADDR_"]):
        if data["PROVINCE_NAME_"][:-1] in data["ADDR_"][:len(data["PROVINCE_NAME_"])]:
            if data["CITY_NAME_"] == "吉林市" and ("吉林省" not in data["ADDR_"]):
                addr_ = data["PROVINCE_NAME_"] + data["ADDR_"]
            else:
                addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])].
                         replace(data["PROVINCE_NAME_"][:-1], data["PROVINCE_NAME_"]) +
                         data["ADDR_"][len(data["PROVINCE_NAME_"]):])
        elif (data["PROVINCE_NAME_"][:3] in data["ADDR_"][:len(data["PROVINCE_NAME_"])]) and \
                (data["CITY_NAME_"] in data["ADDR_"]):
            addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])].
                     replace(data["PROVINCE_NAME_"][:3], data["PROVINCE_NAME_"]) +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]):])
        elif (data["PROVINCE_NAME_"][:2] in data["ADDR_"][:len(data["PROVINCE_NAME_"])]) and\
                (data["CITY_NAME_"] in data["ADDR_"]):
            addr_ = (data["ADDR_"][:len(data["PROVINCE_NAME_"])].
                     replace(data["PROVINCE_NAME_"][:2], data["PROVINCE_NAME_"]) +
                     data["ADDR_"][len(data["PROVINCE_NAME_"]):])
        else:
            addr_ = data["PROVINCE_NAME_"] + data["ADDR_"]

    # 地址中没有省级没有市级
    elif (data["PROVINCE_NAME_"] not in data["ADDR_"]) and (data["CITY_NAME_"] not in data["ADDR_"]):
        if data["CITY_NAME_"][:-1] in data["ADDR_"][:len(data["CITY_NAME_"])]:
            addr_ = (data["PROVINCE_NAME_"] +
                     data["ADDR_"][:len(data["CITY_NAME_"])].replace(data["CITY_NAME_"][:-1], data["CITY_NAME_"]) +
                     data["ADDR_"][len(data["CITY_NAME_"]):])
        elif data["CITY_NAME_"][:3] in data["ADDR_"][:len(data["CITY_NAME_"])]:
            addr_ = (data["PROVINCE_NAME_"] +
                     data["ADDR_"][:len(data["CITY_NAME_"])].replace(data["CITY_NAME_"][:3], data["CITY_NAME_"]) +
                     data["ADDR_"][len(data["CITY_NAME_"]):])
        elif data["CITY_NAME_"][:2] in data["ADDR_"][:len(data["CITY_NAME_"])]:
            addr_ = (data["PROVINCE_NAME_"] +
                     data["ADDR_"][:len(data["CITY_NAME_"])].replace(data["CITY_NAME_"][:2], data["CITY_NAME_"]) +
                     data["ADDR_"][len(data["CITY_NAME_"]):])
        else:
            addr_ = data["PROVINCE_NAME_"] + data["CITY_NAME_"] + data["ADDR_"]

    # 地址中有区县级
    if data["AREA_NAME_"] in addr_:
        pass
    # 直辖市
    elif data["CITY_CODE_"] == data["PROVINCE_CODE_"]:
        if data["AREA_NAME_"][:-1] in addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])]:
            addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])].
                     replace(data["AREA_NAME_"][:-1], data["AREA_NAME_"]) +
                     addr_[len(data["PROVINCE_NAME_"])+len(data["AREA_NAME_"]):])
        elif data["AREA_NAME_"][:4] in addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])]:
            addr_ = (addr_[:len(data["PROVINCE_NAME_"])].replace(data["AREA_NAME_"][:4], data["AREA_NAME_"]) +
                     addr_[len(data["PROVINCE_NAME_"])+len(data["AREA_NAME_"]):])
        elif data["AREA_NAME_"][:3] in addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])]:
            addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])].
                     replace(data["AREA_NAME_"][:3], data["AREA_NAME_"]) +
                     addr_[len(data["PROVINCE_NAME_"])+len(data["AREA_NAME_"]):])
        elif data["AREA_NAME_"][:2] in addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])]:
            addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"])].
                     replace(data["AREA_NAME_"][:2], data["AREA_NAME_"]) +
                     addr_[len(data["PROVINCE_NAME_"])+len(data["AREA_NAME_"]):])
        else:
            addr_ = (addr_[:len(data["PROVINCE_NAME_"])] + data["AREA_NAME_"] +
                     addr_[len(data["PROVINCE_NAME_"]):])
    # 非直辖市
    elif (data["AREA_NAME_"] == "城区") or (data["AREA_NAME_"] == "郊区"):
        addr_ = addr_.replace(data["AREA_NAME_"], "")
    elif (data["AREA_NAME_"][:-1] in
          addr_[:len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]) + len(data["AREA_NAME_"])]):
        addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"])+len(data["AREA_NAME_"])].
                 replace(data["AREA_NAME_"][:-1], data["AREA_NAME_"]) +
                 addr_[len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"]):])
    elif (data["AREA_NAME_"][:4] in
          addr_[:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])+len(data["AREA_NAME_"])]):
        addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]) + len(data["AREA_NAME_"])].
                 replace(data["AREA_NAME_"][:4], data["AREA_NAME_"]) +
                 addr_[len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"]):])
    elif (data["AREA_NAME_"][:3] in
          addr_[:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])+len(data["AREA_NAME_"])]):
        addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]) + len(data["AREA_NAME_"])].
                 replace(data["AREA_NAME_"][:3], data["AREA_NAME_"]) +
                 addr_[len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"]):])
    elif (data["AREA_NAME_"][:2] in
          addr_[:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])+len(data["AREA_NAME_"])]):
        addr_ = (addr_[:len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]) + len(data["AREA_NAME_"])].
                 replace(data["AREA_NAME_"][:2], data["AREA_NAME_"]) +
                 addr_[len(data["PROVINCE_NAME_"]) + len(data["AREA_NAME_"]):])
    else:
        if len(data["AREA_NAME_"]) > 3:
            addr_ = (addr_[:len(data["PROVINCE_NAME_"])+len(data["CITY_NAME_"])] +
                     data["AREA_NAME_"] + addr_[len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"]):])

    # 剩余数据在数据库中无区县级
    if not addr_:
        if data["PROVINCE_NAME_"] not in data["ADDR_"][:len(data["PROVINCE_NAME_"])]:
            data["ADDR_"] = data["PROVINCE_NAME_"] + data["ADDR_"]
        if data["CITY_NAME_"] not in data["ADDR_"][:len(data["PROVINCE_NAME_"]) + len(data["CITY_NAME_"])]:
            data["ADDR_"] = (data["ADDR_"][:len(data["PROVINCE_NAME_"])] + data["CITY_NAME_"] +
                             data["ADDR_"][len(data["PROVINCE_NAME_"]):])
        addr_ = data["ADDR_"]
        # data["AREA_CODE_"] = data["CITY_CODE_"]

    if "直辖" in data["CITY_NAME_"]:
        addr_ = data["ADDR_"]

    # # 添加分行编码
    # branch_code = None
    # for i in range(1, 10000):
    #     branch_code = "ABC" + "_" + data["CITY_CODE_"] + "_" + "00000"
    #     branch_code = branch_code[:len(branch_code)-len(str(i))] + "{}".format(i)
    #     if branch_code in branch_code_list:
    #         continue
    #     else:
    #         branch_code_list.append(branch_code)
    #         break

    # re_data["_id"] = data["_id"]
    # "C"
    re_data["BANK_CODE_"] = "ABC"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-3]
    # re_data["AREA_CODE_"] = data["AREA_CODE_"]
    # re_data["AREA_NAME_"] = data["AREA_NAME_"]
    # re_data["UNIT_CODE_"] = "ABC" + "_" + data["CITY_CODE_"]

    # "F"
    re_data["ADDR_"] = addr_
    # re_data["CITY_CODE_"] = data["CITY_CODE_"]
    # re_data["CITY_NAME_"] = data["CITY_NAME_"]
    # re_data["LAT_"] = data["LAT_"]
    # re_data["LNG_"] = data["LNG_"]

    re_data["NAME_"] = data["NAME_"]
    # re_data["PROVINCE_CODE_"] = data["PROVINCE_CODE_"][:4]
    # re_data["PROVINCE_NAME_"] = data["PROVINCE_NAME_"]
    # re_data["DISTRICT_CODE_"] = data["AREA_CODE_"]
    # re_data["DISTRICT_NAME_"] = data["AREA_NAME_"]
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
    re_data["UNIT_CODE_"] = "ABC" + "_" + re_data.get("CITY_CODE_", "")

    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    re_data["URL_"] = data["URL_"]
    re_data["TEL_"] = data.get("TEL_", "")

    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
        if re_data["SOURCE_TYPE_NAME_"] == "营业网点":
            re_data["TYPE_NAME_"] = "支行"
            re_data["TYPE_"] = "ZH"
        else:
            re_data["TYPE_NAME_"] = "自助银行"
            re_data["TYPE_"] = "ZZ"
    if "BUSINESS_HOURS_" in data:
        re_data["BUSINESS_HOURS_"] = data["BUSINESS_HOURS_"]

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ABCORGANIZE", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
