# -*- coding: utf-8 -*-

# 同花顺财经财报数据信息 10JQKAFINACIAL

from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="BOCFinancial", entity_type="FINPRODUCT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
