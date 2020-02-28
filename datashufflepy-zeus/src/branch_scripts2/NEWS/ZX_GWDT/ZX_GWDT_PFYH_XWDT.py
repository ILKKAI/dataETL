# -*- coding: utf-8 -*-
""""ZX_GWDT_PFYH_XWDT": "浦发银行|SPDB","""
from database._mongodb import MongoClient


def data_shuffle(data):
    data["BANK_NAME_"] = "浦发银行"
    data["BANK_CODE_"] = "SPDB"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="ZX_GWDT_PFYH_XWDT", mongo_collection="ZX_GWDT")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
