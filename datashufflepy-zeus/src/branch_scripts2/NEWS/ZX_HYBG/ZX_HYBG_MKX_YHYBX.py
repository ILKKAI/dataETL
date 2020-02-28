# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    if "AUTHOR_" not in data:
        if "作者" in data["CONTENT_"][:2]:
            author_content = re.findall(r"作者：(.*?)\|", data["CONTENT_"])
            if author_content:
                author_list = author_content[0].split("、")
            else:
                author_list = []
        else:
            author_content = re.findall(r"作者：\|?(.*)", data["CONTENT_"])
            if author_content:
                author_list = re.findall(r"\|\w+?[,，|]", author_content[0])
            else:
                author_list = []
        if author_list:
            data["AUTHOR_"] = "|".join(author_list)

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_HYBG_MKX_YHYBX", mongo_collection="ZX_HYBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
