# -*- coding: utf-8 -*-
"""链家-小区-小区租赁-南宁 WD_JZ_FJ_LIXQZL_NN"""
import re


def data_shuffle(data):
    # 住宅名称
    if "·" in data["NAME_"] or "·" in data["NAME_"]:
        house_name = re.findall(r"[\u4e00-\u9fa5]{2}[^\w]([\w()\-（）.,，]+)\|", data["NAME_"])
    else:
        house_name = re.findall(r"\|([\w()\-（）.,，]+)\|", data["NAME_"])
        # print(house_name)
    if house_name:
        data["TITLE_"] = data["NAME_"]
        data["NAME_"] = house_name[0]
    # print(house_name)
    return data

