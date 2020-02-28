# -*- coding: utf-8 -*-
"""广发银行 官网动态 ZX_GWDT_GFYH_GFXW"""
""""ZX_GWDT_GFYH_GFXW": "广发银行|CGB","""
import re
from database._mongodb import MongoClient


def data_shuffle(data):
    if data["CONTENT_"]:
        data["CONTENT_"] = re.sub(r"/\*[^\u4e00-\u9fa5]+", "", data["CONTENT_"], count=1)
    if data["HTML_"]:
        data["HTML_"] = re.sub("<p[^>]+align=\"center\">.*?</p>", "", data["HTML_"])
    data["BANK_NAME_"] = "广发银行"
    data["BANK_CODE_"] = "CGB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_GFYH_GFXW", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)


