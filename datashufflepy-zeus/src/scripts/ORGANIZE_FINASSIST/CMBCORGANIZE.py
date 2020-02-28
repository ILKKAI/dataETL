# -*- coding: utf-8 -*-

#  中国民生银行网点 CMBCORGANIZE
# 新添加  无数据

import hashlib

from scripts import GenericScript


def data_shuffle(data, province_list, city_list, area_list):
    re_data = dict()
    prov_n = None
    prov_c = None
    city_n = None
    city_c = None
    area_n = None
    area_c = None
    addr_ = None

    # # 添加分行编码
    # branch_code = None
    # for i in range(1, 10000):
    #     branch_code = "CMBC" + "_" + city_c + "_" + "00000"
    #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
    #     if branch_code in branch_code_list:
    #         continue
    #     else:
    #         branch_code_list.append(branch_code)
    #         break

    # "C"
    hash_m = hashlib.md5()
    hash_m.update(data["NAME_"].encode("utf-8"))
    hash_title = hash_m.hexdigest()
    re_data["ID_"] = (data["ENTITY_CODE_"] + "_" +
                      str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(hash_title))
    re_data["BANK_CODE_"] = "CMBC"
    re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    re_data["AREA_CODE_"] = area_c
    re_data["UNIT_CODE_"] = "CMBC" + "_" + city_c

    # "F"
    re_data["ADDR_"] = addr_
    re_data["CITY_CODE_"] = city_c
    re_data["CITY_"] = city_n
    re_data["LAT_"] = data["LAT_"]
    re_data["LNG_"] = data["LNG_"]
    re_data["NAME_"] = data["NAME_"]
    re_data["PROVINCE_CODE_"] = prov_c
    re_data["PROVINCE_NAME_"] = prov_n
    re_data["DISTRICT_CODE_"] = area_c
    re_data["DISTRICT_NAME_"] = area_n
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["URL_"] = data["URL_"]

    # "S"
    re_data["STATUS_1"] = "1"

    return re_data


def run():
    script = GenericScript(entity_code="CMBCORGANIZE", entity_type="ORGANIZE_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    batch_list = data_shuffle(mongo_data_list, province_list, city_list, area_list)


if __name__ == '__main__':
    run()
