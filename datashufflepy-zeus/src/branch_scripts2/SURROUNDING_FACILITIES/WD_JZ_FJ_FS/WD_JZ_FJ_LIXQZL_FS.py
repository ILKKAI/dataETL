# -*- coding: utf-8 -*-
"""链家-小区-小区租赁-佛山 WD_JZ_FJ_LIXQZL_FS"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    # 住宅名称
    data['NAME_'] = data['NAME_'].replace('整租·', '')
    data['NAME_'] = data['NAME_'].replace('独栋·', '')
    # print(data['NAME_'])
    try:
        data['NAME_'] = data['NAME_'][:data['NAME_'].index('室') -1]
    except:
        pass

    if "·" in data["NAME_"] or "·" in data["NAME_"]:
        house_name = re.findall(r"[\u4e00-\u9fa5]{2}[^\w]([\w()\-（）.,，]+)\|", data["NAME_"])
    else:
        house_name = re.findall(r"\|([\w()\-（）.,，]+)\|", data["NAME_"])
        # print(house_name)
    data["TITLE_"] = data["NAME_"]
    if house_name:
        data["NAME_"] = house_name[0]
    # print(house_name)
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="WD_JZ_FJ_LIXQZL_FS", mongo_collection="WD_JZ_FJ_FS")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
