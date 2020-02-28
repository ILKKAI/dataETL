# -*- coding: utf-8 -*-
"""链家-写字楼-租赁-成都 WD_JZ_FJ_LIXZLZL_CD"""
import re


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
