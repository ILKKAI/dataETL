# -*- coding: utf-8 -*-

# 浙商银行网点 CZBORGANIZE  无经纬度

# 已处理 2(450条)
import hashlib
import re

from hbase.ttypes import Mutation, BatchMutation
from scripts import GenericScript


def data_shuffle(data, province_list, city_list, area_list):
    data_list = list()

    for city in city_list:
        if city["NAME_"] == "县":
            city_list.remove(city)

    # 正则规则
    pattern = re.compile(r"(.*?)\|0.*?\|0.*?\|")

    # 将各个网点从大文本中分离
    first_shuffle = data["CONTENT_"].split("周末营业|")[1]

    second_shuffle = pattern.findall(first_shuffle)
    tel_shuffle = re.findall(r"\d{3,4}-*\d{5,9}", first_shuffle)
    re_each = list()
    # 将分离的网点信息存入 re_each 中
    for each in second_shuffle:
        each = each.split("|")
        re_eac = list()
        for eac in each:
            if eac == "√" or eac[:1] == "仅" or eac[:1] == "(" or eac[:1] == "非":
                continue
            else:
                re_eac.append(eac)
        re_each.append(re_eac)

    # 按列表长度判断是否网点信息
    count = 0
    for bank in re_each:
        re_data = dict()
        prov_n = ""
        prov_c = ""
        city_n = ""
        city_c = ""
        area_n = ""
        area_c = ""
        name = bank[0]
        addr_ = bank[-1]
        tel = ",".join(tel_shuffle[count:count+2])
        count += 2

        # 特殊情况
        if "中国（四川）自由贸易试验区" in bank[1]:
            addr_ = addr_.replace("中国（四川）自由贸易试验区", "")

        for area in area_list:
            if len(area["NAME_"]) < 3:
                continue
            elif area["NAME_"] in addr_:
                area_n = area["NAME_"]
                area_c = area["CODE_"]
                city_c = area["PARENT_"]
                continue

        for city in city_list:
            if city["NAME_"] in addr_:
                city_n = city["NAME_"]
                city_c = city["CODE_"]
                prov_c = city["PARENT_"]
                break
            elif city["CODE_"] == city_c:
                city_n = city["NAME_"]
                prov_c = city["PARENT_"]
        if not city_n:
            for city in city_list:
                if city["NAME_"][:2] in addr_:
                    city_n = city["NAME_"]
                    city_c = city["CODE_"]
                    prov_c = city["PARENT_"]

        for prov in province_list:
            if prov["NAME_"][:2] in addr_[:len(prov["NAME_"])]:
                prov_n = prov["NAME_"]
                prov_c = prov["CODE_"]
            elif prov["CODE_"] == prov_c:
                prov_n = prov["NAME_"]

        if ("北京" in prov_n) or ("上海" in prov_n) or ("天津" in prov_n) or ("重庆" in prov_n):
            city_n = prov_n
            city_c = prov_c

        # 地址清洗
        if prov_n in addr_:
            pass
        elif prov_n[:-1] in addr_[:len(prov_n)]:
            addr_ = addr_[:len(prov_n)].replace(prov_n[:-1], prov_n) + addr_[len(prov_n):]
        elif prov_n[:4] in addr_[:len(prov_n)]:
            addr_ = addr_[:len(prov_n)].replace(prov_n[:4], prov_n) + addr_[len(prov_n):]
        elif prov_n[:3] in addr_[:len(prov_n)]:
            addr_ = addr_[:len(prov_n)].replace(prov_n[:3], prov_n) + addr_[len(prov_n):]
        elif prov_n[:2] in addr_[:len(prov_n)]:
            addr_ = addr_[:len(prov_n)].replace(prov_n[:2], prov_n) + addr_[len(prov_n):]
        else:
            addr_ = prov_n + addr_

        if city_n in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_
        elif city_n[:-1] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:-1], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:4] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:4], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:3] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:3], city_n) + addr_[len(prov_n) + len(city_n):]
        elif city_n[:2] in addr_[:len(prov_n) + len(city_n)]:
            addr_ = addr_[:len(prov_n) + len(city_n)].replace(
                city_n[:2], city_n) + addr_[len(prov_n) + len(city_n):]
        else:
            addr_ = addr_[:len(prov_n)] + city_n + addr_[len(prov_n):]

        # "C"
        hash_m = hashlib.md5()
        hash_m.update(bank[0].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        re_data["ID_"] = (data["ENTITY_CODE_"] + "_" +
                          str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(hash_title))
        re_data["BANK_CODE_"] = "CZB"
        re_data["BANK_NAME_"] = data["ENTITY_NAME_"][:-2]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        re_data["AREA_CODE_"] = area_c
        re_data["UNIT_CODE_"] = "CZB" + "_" + city_c

        # "F"
        re_data["ADDR_"] = addr_
        re_data["CITY_CODE_"] = city_c
        re_data["CITY_"] = city_n
        re_data["LAT_"] = ""
        re_data["LNG_"] = ""
        re_data["NAME_"] = name
        re_data["PROVINCE_CODE_"] = prov_c
        re_data["PROVINCE_NAME_"] = prov_n
        re_data["DISTRICT_CODE_"] = area_c
        re_data["DISTRICT_NAME_"] = area_n
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["URL_"] = data["URL_"]
        re_data["TEL_"] = tel
        re_data["BUSINESS_HOURS_"] = "周一至周五"

        # "S"
        re_data["STATUS_1"] = "1"

        data_list.append(re_data)

    return data_list


def run():
    script = GenericScript(entity_code="CZBORGANIZE", entity_type="ORGANIZE_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    batch_list = data_shuffle(mongo_data_list, province_list, city_list, area_list)


if __name__ == '__main__':
    run()
