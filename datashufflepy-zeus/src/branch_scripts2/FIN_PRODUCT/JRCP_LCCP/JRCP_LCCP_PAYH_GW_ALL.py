# -*- coding: utf-8 -*-
"""平安银行 定期理财产品 JRCP_LCCP_PAYH_GW_ALL"""
from database._mongodb import MongoClient


def data_shuffle(data):
    if "PDF_NAME_" in data:
        if isinstance(data["PDF_NAME_"], (list, tuple)):
            pdf_name = data["PDF_NAME_"]
            for each in pdf_name:
                if "说明书" in each:
                    pdf_index = data["PDF_NAME_"].index(each)
                    data["PDF_"] = data["PDF_"][pdf_index]
                    data["PDF_NAME_"] = data["PDF_NAME_"][pdf_index]
                    break
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_PAYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
