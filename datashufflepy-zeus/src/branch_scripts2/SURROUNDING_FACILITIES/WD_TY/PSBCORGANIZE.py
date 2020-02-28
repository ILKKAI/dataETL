# -*- coding: utf-8 -*-
"""中国邮政储蓄银行网点 PSBCORGANIZE"""
from branch_scripts import GenericScript
from database._mongodb import MongoClient


def data_shuffle(mongo_data_list, province_list, city_list, area_list):
    data_list = list()
    return None


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="PSBCORGANIZE", mongo_collection="WD_TY")
    sc = GenericScript
    # Mysql connection
    sc.mysql_client, sc.mysql_connection = sc.mysql_connect()
    province_list, city_list, area_list, dir_area_list, bank_list = sc.data_from_mysql()
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data, province_list, city_list, area_list)
        # print(re_data)
