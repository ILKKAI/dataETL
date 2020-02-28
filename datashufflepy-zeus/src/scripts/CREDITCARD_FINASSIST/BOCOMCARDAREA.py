# -*- coding: utf-8 -*-

# 交通银行信用卡专区 BOCOMCARDAREA

from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="BOCOMCARDAREA", entity_type="CREDITCARD_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
