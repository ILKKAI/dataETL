# -*- coding: utf-8 -*-
"""中国建设银行 ZX_GWDT_JSYH_DTJJ"""
""""ZX_GWDT_JSYH_DTJJ": "中国建设银行|CCB","""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data["CONTENT_"]:
        data["CONTENT_"] = re.sub("<h2>.*?</h2>", "", data["CONTENT_"])
        data["CONTENT_"] = re.sub("<div class=\"time\">.*?</div>", "", data["CONTENT_"])
    data["BANK_NAME_"] = "中国建设银行"
    data["BANK_CODE_"] = "CCB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_JSYH_DTJJ", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
