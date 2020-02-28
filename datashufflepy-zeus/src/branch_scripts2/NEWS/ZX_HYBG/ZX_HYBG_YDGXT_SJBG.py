# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    data["HTML_"] = re.sub(r"<div class=\"report-verify-container\">.*?</div>", "", data["HTML_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_HYBG_YDGXT_SJBG", mongo_collection="ZX_HYBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
