# -*- coding: utf-8 -*-

# 渤海银行网点 CBHBORGANIZE  无经纬度
# 已处理 100(522条)
import hashlib

from hbase.ttypes import Mutation, BatchMutation
from scripts import GenericScript


def data_shuffle(data, province_list, city_list, area_list):
    data_list = list()

    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    prov_name = ""
    prov_code = ""
    city_name = ""
    city_code = ""
    # 一个地区网点在一条数据中，先将各个网点提取
    organize = list()

    first_shuffle = data["CONTENT_"].split("取款机|")[1]

    for prov in province_list:
        # 直辖市
        if prov["NAME_"][:2] == first_shuffle[:2]:
            prov_name = prov["NAME_"]
            prov_code = prov["CODE_"]
            city_name = prov["NAME_"]
            city_code = prov["CODE_"]
    # 非直辖市
    if city_name is None:
        for city in city_list:
            if city["NAME_"][:2] == first_shuffle[:2]:
                city_name = city["NAME_"]
                city_code = city["CODE_"]

                for prov in province_list:
                    if prov["CODE_"][:2] == city_code[:2]:
                        prov_name = prov["NAME_"]
                        prov_code = prov["CODE_"]

    if city_name is None:
        for city in city_list:
            if city["NAME_"][:2] in first_shuffle[:20]:
                city_name = city["NAME_"]
                city_code = city["CODE_"]

                for prov in province_list:
                    if prov["CODE_"][:2] == city_code[:2]:
                        prov_name = prov["NAME_"]
                        prov_code = prov["CODE_"]

    second_shuffle = first_shuffle.split("|√|7×24小时|")

    for da in second_shuffle:
        organize_ = dict()
        data_ = da.split("|")

        for d in data_:
            if d == "√":
                continue
            elif d[:1] in "0123456789":
                organize[-1]["TEL_"] = d
            elif "分行" in d or "支行" in d:
                organize_["NAME_"] = d
            else:
                organize_["ADDR_"] = d
        organize.append(organize_)
    organize.pop()

    for orga in organize:
        re_data = dict()

        area_name = ""
        area_code = ""
        for area in area_list:
            if (area["CODE_"][:2] == city_code[:2]) and (area["NAME_"] in orga["ADDR_"]):
                area_name = area["NAME_"]
                area_code = area["CODE_"]
        if area_code is None:
            for area in area_list:
                if (area["CODE_"][:2] == city_code[:2]) and (area["NAME_"][:2] in orga["NAME_"]):
                    area_name = area["NAME_"]
                    area_code = area["CODE_"]

        if area_code is None:
            area_name = city_name
            area_code = city_code

        address_ = None
        if prov_name in orga["ADDR_"][:len(prov_name)]:
            if city_name in orga["ADDR_"][len(prov_name)-1:len(prov_name) + len(city_name)]:
                address_ = orga["ADDR_"]
            else:
                address_ = orga["ADDR_"]

        if address_ is None:
            if prov_name[:-1] in orga["ADDR_"][:len(prov_name)]:
                address_ = (orga["ADDR_"][:len(prov_name)].replace(prov_name[:-1], prov_name) +
                            orga["ADDR_"][len(prov_name):])

        if address_ is None:
            if city_name in orga["ADDR_"][:len(city_name)]:
                address_ = prov_name + orga["ADDR_"]

        if address_ is None:
            if city_name[:2] in orga["ADDR_"][:len(city_name)]:
                address_ = (prov_name + orga["ADDR_"][:len(city_name)].replace(city_name[:2], city_name) +
                            orga["ADDR_"][len(city_name):])

        if address_ is None:
            address_ = prov_name + city_name + orga["ADDR_"]
            if address_[:3] == address_[3:6]:
                address_ = address_.replace(address_[:3], "", 1)

        # # 分行编码
        # branch_code = None
        # for i in range(1, 10000):
        #     branch_code = "CBHB" + "_" + area_code + "_" + "00000"
        #     branch_code = branch_code[:len(branch_code) - len(str(i))] + "{}".format(i)
        #     if branch_code in branch_code_list:
        #         continue
        #     else:
        #         branch_code_list.append(branch_code)
        #         data["UNIT_CODE_"] = branch_code
        #         break

        # "C"
        hash_m = hashlib.md5()
        hash_m.update(orga["NAME_"].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        re_data["ID_"] = (data["ENTITY_CODE_"] + "_" +
                          str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(hash_title))
        re_data["BANK_CODE_"] = "CBHB"
        re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        re_data["AREA_CODE_"] = area_code
        re_data["UNIT_CODE_"] = "CBHB" + city_code

        # "F"
        re_data["ADDR_"] = address_
        re_data["LAT_"] = ""
        re_data["LNG_"] = ""
        re_data["NAME_"] = orga["NAME_"]
        re_data["PROVINCE_CODE_"] = prov_code
        re_data["PROVINCE_NAME_"] = prov_name
        re_data["CITY_CODE_"] = city_code
        re_data["CITY_"] = city_name
        re_data["DISTRICT_CODE_"] = area_code
        re_data["DISTRICT_NAME_"] = area_name
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["URL_"] = data["URL_"]
        re_data["TEL_"] = orga["TEL_"]
        re_data["BUSINESS_HOURS_"] = "7×24小时"

        # "S"
        re_data["STATUS_1"] = "1"

        data_list.append(re_data)

    return data_list


def run():
    script = GenericScript(entity_code="CBHBORGANIZE", entity_type="ORGANIZE_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    batch_list = data_shuffle(mongo_data_list, province_list, city_list, area_list)


if __name__ == '__main__':
    run()
