# -*- coding: utf-8 -*-
from crm_scripts import GenericScript
from database._mongodb import MongoClient

mysql_client, mysql_connection = GenericScript.mysql_connect()
province_list, city_list, area_list, dir_area_list, bank_list = GenericScript.data_from_mysql(mysql_client=mysql_client, mysql_connection=mysql_connection)


def data_shuffle(data, ):
    if "SALE_AREA_" in data:
        if data["SALE_AREA_"]:
            sale_area_list = list()
            code_list = data["SALE_AREA_"].split(",")
            for prov in province_list:
                if prov["CODE_"] + "00" in code_list:
                    sale_area_list.append(prov["NAME_"])
            for city in city_list:
                if city["CODE_"] in code_list:
                    sale_area_list.append(city["NAME_"])
            if sale_area_list:
                data["SALE_AREA_"] = "|".join(sale_area_list)

    return data


# 查看香港澳门能否出来,, 查询有新修改后的字段的数据进行 清洗
if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="CRMJPFX_LCCP_ZGLCW", mongo_collection="CRMJPFX_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
