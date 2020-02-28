# -*- coding: utf-8 -*-
"""顺德农商银行网点 WD_TY_SDNSYH_GW_ALL  """
from crm_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()

    # 省级信息清洗
    data["PROVINCE_NAME_"] = '广东'
    for prov in province_list:
        if prov["NAME_"][:2] in data["PROVINCE_NAME_"]:
            re_data["PROVINCE_NAME_"] = prov["NAME_"]
            re_data["PROVINCE_CODE_"] = prov["CODE_"]
            break

    # 市级信息清洗
    data["CITY_NAME_"] = '佛山市'
    for city in city_list:
        if city["CODE_"][:2] == re_data["PROVINCE_CODE_"][:2]:
            if city["NAME_"][:2] in data["CITY_NAME_"]:
                re_data["CITY_NAME_"] = city["NAME_"]
                re_data["CITY_CODE_"] = city["CODE_"]
                break

    # 区县级信息清洗
    data["AREA_NAME_"] = '顺德区'
    for area in area_list:
        if area["CODE_"][:2] == re_data["PROVINCE_CODE_"][:2]:
            if area["NAME_"] == data["AREA_NAME_"]:
                area_n = area["NAME_"]
                area_c = area["CODE_"]
            elif area["NAME_"][:-1] == data["AREA_NAME_"][:-1]:
                area_n = area["NAME_"]
                area_c = area["CODE_"]

        # 地址清洗
        prov_n = re_data["PROVINCE_NAME_"]
        city_n = re_data["CITY_NAME_"]
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

        # 将市的名称补全
        if city_n in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_
        elif city_n[:-1] in addr_[:len(prov_n) + len(city_n)]:  # '市' 缺失 ,添加市
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(city_n[:-1], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:4] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(city_n[:4], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:3] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(city_n[:3], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:2] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(city_n[:2], city_n) + addr_[len(prov_n) + len(city_n):]
        else:
            addr_ = addr_[:len(prov_n)] + city_n + addr_[len(prov_n):]

    # "C"
    re_data["BANK_CODE_"] = "SRCB"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-3]
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    # re_data["UNIT_CODE_"] = "CBHB" + re_data.get("CITY_CODE_", "")

    # "F"
    re_data["ADDR_"] = data["ADDR_"]
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
    re_data["UNIT_CODE_"] = "SRCB" + "_" + re_data.get("CITY_CODE_", "")

    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["URL_"] = data["URL_"]
    if "TEL_" in data:
        re_data["TEL_"] = data["TEL_"]
    re_data["BUSINESS_HOURS_"] = "0:00-24:00"
    if "SOURCE_TYPE_NAME_" in data:
        re_data["SOURCE_TYPE_NAME_"] = data["SOURCE_TYPE_NAME_"]
    re_data["TYPE_NAME_"] = "支行"
    re_data["TYPE_"] = "ZH"
    return re_data


if __name__ == '__main__':
    m_client, connection = GenericScript.mysql_connect({"host": "172.22.69.41", "port": 3306, "database": "chabei_creditcard", "user": "root", "password": "dev007%P", "charset": "utf8", 'table': 'cha_di_position'

    },)
    province_list, city_list, area_list, dir_area_list, bank_list = \
        GenericScript.data_from_mysql(mysql_client=m_client, mysql_connection=connection)

    main_mongo = MongoClient(entity_code="WD_TY_SDNSYH_GW_ALL", mongo_collection="WD_TY")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data=data, province_list=province_list, city_list=city_list, area_list=area_list)
        print(re_data)
