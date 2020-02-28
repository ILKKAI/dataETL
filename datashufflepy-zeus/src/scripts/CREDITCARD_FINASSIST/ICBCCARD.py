# -*- coding: utf-8 -*-

# 工商银行信用卡 ICBCCARD

from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="ICBCCARD", entity_type="CREDITCARD_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
