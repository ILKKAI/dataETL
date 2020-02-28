# -*- coding: utf-8 -*-
"""易观智库-行业观察 ZX_HYBG_YGZK_HYDC"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data["HTML_"]:
        data["HTML_"] = re.sub("\n", "", data["HTML_"])
        data["HTML_"] = re.sub(r"<div class=\"payinfo\" style=\"margin-top: 20px\">.*?立即下载数字化转型最佳实践.*?</div>.*?</div>.*?</div>", "", data["HTML_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_HYBG_YGZK_HYDC", mongo_collection="ZX_HYBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
