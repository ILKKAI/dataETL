# -*- coding: utf-8 -*-
import os
from database._mongodb import MongoClient


def data_shuffle(data):
    com_list = ["光大永明人寿保险公司", "太平人寿保险公司", "中信保诚人寿保险公司"]
    if data.get("PRO_NAME"):
        data["PRO_NAME_"] = data["PRO_NAME"]
        for com_name in com_list:
            if data["PRO_NAME_"][:2] == com_name[:2]:
                data["COM_NAME_"] = com_name
        curPath = os.path.abspath(os.path.dirname(__file__))
        data["LOCAL_PDF_PATH_"] = "".join([curPath, "/渤海代理保险合同条款/", data["PRO_NAME_"].replace("产品计划", ""), ".pdf"])
        data["LOCAL_PDF_NAME_"] = data["PRO_NAME_"] + "条款"
        if data.get("PDF_"):
            del data["PDF_"]

        return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_BHYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
