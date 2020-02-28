# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    # result = re.sub(r"<div class=\"news_audio\">.*?</div>", "", data["CONTENT_HTML_"])
    # result = re.sub(r"<span.*?音频来源股市广播</span>", "", result)
    data["HTML_"] = re.sub(r"<p class=\"copyright\".*?</p>", "", data["HTML_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_CJXW_ZYCJ_21SJJJW", mongo_collection="ZX_CJXW_ZYCJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
