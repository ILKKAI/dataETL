# -*- coding: utf-8 -*-
"""艾瑞资讯 ZX_HYBG_ARZX_CYYJBG"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data['HTML_']:
        data['HTML_'] = re.sub("\n", "", data['HTML_'])
        data['HTML_'] = re.sub(r"<h1 class=\"tit\">.*?</h1>", "", data['HTML_'])
        data['HTML_'] = re.sub(r"<a.*?class=\"btn btn-auto btn-down\".*?</a>", "", data['HTML_'])
        data['HTML_'] = re.sub(r"<div class=\"info\">.*?</div>", "", data['HTML_'])
        data["HTML_"] = data['HTML_'].replace("e111", "")

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_HYBG_ARZX_CYYJBG", mongo_collection="ZX_HYBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
