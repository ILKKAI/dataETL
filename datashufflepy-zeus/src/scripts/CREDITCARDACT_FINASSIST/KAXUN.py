# -*- coding: utf-8 -*-

# 卡讯网优惠活动 KAXUN

from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="KAXUN", entity_type="CREDITCARDACT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
