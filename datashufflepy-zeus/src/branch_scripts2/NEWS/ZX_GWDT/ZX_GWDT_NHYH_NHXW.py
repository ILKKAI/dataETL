# -*- coding: utf-8 -*-
"""南海农商银行 官网动态 ZX_GWDT_NHYH_NHXW"""
import re
from database._mongodb import MongoClient


def data_shuffle(data):
    # if data["CONTENT_"]:
    #     data["CONTENT_"] = re.sub(r"/\*[^\u4e00-\u9fa5]+", "", data["CONTENT_"], count=1)
    # if data["HTML_"]:
    #     data["HTML_"] = re.sub("<p[^>]+align=\"center\">.*?</p>", "", data["HTML_"])
    data["BANK_NAME_"] = "南海农商银行"
    data["BANK_CODE_"] = "NRCB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_NHYH_NHXW", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)


