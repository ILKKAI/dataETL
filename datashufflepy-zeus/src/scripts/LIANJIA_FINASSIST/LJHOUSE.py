# -*- coding: utf-8 -*-

# 链家网小区信息 LJHOUSE


from scripts import GenericScript


def data_shuffle():
    pass


def run():
    script = GenericScript(entity_code="BOCOMCity", entity_type="ORGANIZE_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    data_shuffle()
    script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
