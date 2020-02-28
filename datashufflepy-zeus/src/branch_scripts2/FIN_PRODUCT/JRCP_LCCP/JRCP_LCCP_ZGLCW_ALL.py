# -*- coding: utf-8 -*-
from branch_scripts import GenericScript
from database._mongodb import MongoClient

mysql_client, mysql_connection = GenericScript.mysql_connect()
province_list, city_list, area_list, dir_area_list, bank_list = GenericScript.data_from_mysql(mysql_client=mysql_client, mysql_connection=mysql_connection)


def data_shuffle(data):
    if "SALE_AREA_" in data:
        if data["SALE_AREA_"]:
            sale_area_list = list()
            # prov_list = list()
            # pc_list = list()
            # city_list = list()
            # cc_list = list()
            # 110000,120000,210000,220000,310000,210200
            code_list = data["SALE_AREA_"].split(",")
            for prov in province_list:
                if prov["CODE_"] + "00" in code_list:
                    # prov_list.append(prov["NAME_"])
                    # pc_list.append(prov["CODE_"])
                    sale_area_list.append(prov["NAME_"])
            for city in city_list:
                if city["CODE_"] in code_list:
                    # city_list.append(city["NAME_"])
                    # cc_list.append(city["CODE_"])
                    # pc_list.append(city["PARENT_"])
                    sale_area_list.append(city["NAME_"])
            if sale_area_list:
                data["SALE_AREA_"] = "|".join(sale_area_list)
            # if pc_list:
            #     for prov in province_list:
            #         if prov["CODE_"] in pc_list:
            #             prov_list.append(prov["NAME_"])

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGLCW_ALL", mongo_collection="CRMJPFX_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
