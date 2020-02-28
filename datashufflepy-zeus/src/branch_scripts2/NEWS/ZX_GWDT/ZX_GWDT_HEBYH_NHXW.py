# -*- coding: utf-8 -*-
"""河北银行官网动态     ZX_GWDT_HEBYH_NHXW"""
import re
from database._mongodb import MongoClient


def data_shuffle(data):

    data["BANK_NAME_"] = "河北银行"
    data["BANK_CODE_"] = "BHB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_HEBYH_NHXW", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
