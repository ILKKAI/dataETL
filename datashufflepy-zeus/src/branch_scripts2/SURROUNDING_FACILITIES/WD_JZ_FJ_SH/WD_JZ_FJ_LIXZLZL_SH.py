# -*- coding: utf-8 -*-
"""链家-写字楼-租赁-上海 WD_JZ_FJ_LIXZLZL_SH"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    # 写字楼名称
    if "·" in data["NAME_"]:
        house_name = re.findall(r"[\u4e00-\u9fa5]{2}[^\w](\w+)\|", data["NAME_"])
    else:
        house_name = re.findall(r"\|(\w+)\|", data["NAME_"])
    if house_name:
        data["TITLE_"] = data["NAME_"]
        data["NAME_"] = house_name[0]

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="WD_JZ_FJ_LIXZLZL_SH", mongo_collection="WD_JZ_FJ_SH")
    # sc = GenericScript
    # Mysql connection
    # sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    # province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
