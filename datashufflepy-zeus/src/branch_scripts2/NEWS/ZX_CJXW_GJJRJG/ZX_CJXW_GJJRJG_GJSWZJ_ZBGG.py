# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    data["CONTENT_"] = re.sub(r"var[^\u4e00-\u9fa5]+", "", data["CONTENT_"], count=1)
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_CJXW_GJJRJG_GJSWZJ_ZBGG", mongo_collection="ZX_CJXW_HY")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
