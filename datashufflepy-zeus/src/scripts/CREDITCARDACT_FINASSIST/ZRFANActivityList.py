# -*- coding: utf-8 -*-

# 羊毛优惠活动列表 ZRFANActivityList

from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="ZRFANActivityList", entity_type="CREDITCARDACT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
