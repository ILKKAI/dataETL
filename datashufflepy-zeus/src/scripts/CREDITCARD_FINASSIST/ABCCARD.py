# -*- coding: utf-8 -*-

# 农业银行信用卡 ABCCARD

from scripts import GenericScript


def data_shuffle(mongo_data_list):
    for each in mongo_data_list:
        print(each)


def run():
    script = GenericScript(entity_code="ABCCARD", entity_type="CREDITCARD_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle(mongo_data_list)
    # script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
