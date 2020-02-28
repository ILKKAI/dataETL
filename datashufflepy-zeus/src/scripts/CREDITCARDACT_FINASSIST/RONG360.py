# -*- coding: utf-8 -*-

# 融360优惠活动 RONG360

from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="RONG360", entity_type="CREDITCARDACT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
