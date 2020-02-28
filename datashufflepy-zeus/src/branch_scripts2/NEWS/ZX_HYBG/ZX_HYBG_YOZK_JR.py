# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if data["CONTENT_"]:
        data["CONTENT_"] = re.sub(r"\.[^\u4e00-\u9fa5].*?{.*?}|\$\(.*?\).*?;", "", data["CONTENT_"])
    if data["HTML_"]:
        data["HTML_"] = re.sub("\n", "", data["HTML_"])
        data["HTML_"] = re.sub(r"<div class=\"relativeReport clearFix\">.*?</div>.*?</div>.*?</div>.*?</div>", "", data["HTML_"])
        data["HTML_"] = re.sub("<div id=\"weixinewm\".*?</div>", "", data["HTML_"])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_HYBG_YOZK_JR", mongo_collection="ZX_HYBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
