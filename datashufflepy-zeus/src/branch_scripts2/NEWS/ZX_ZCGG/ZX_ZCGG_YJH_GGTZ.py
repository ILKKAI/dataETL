# -*- coding: utf-8 -*-
"""银监会-政策法规 ZX_ZCGG_YJH_GGTZ"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data["HTML_"]:
        data["HTML_"] = re.sub("\n", "", data["HTML_"])
        data["HTML_"] = re.sub(r'<div class="online-desc-con".*?</div>', "", data["HTML_"], re.S)
        data["HTML_"] = re.sub(r'<p class="p\d+" style="margin-bottom:0pt; margin-top:0pt; text-align:center; line-height:150%;.*?</p>', "", data["HTML_"], re.S)
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_ZCGG_YJH_GGTZ", mongo_collection="ZX_ZCGG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
        break