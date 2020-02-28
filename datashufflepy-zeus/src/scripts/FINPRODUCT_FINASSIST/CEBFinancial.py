# -*- coding: utf-8 -*-

# 中国光大银行理财产品 CEBFinancial

# 无数据

# 无 风险等级 RISK_LEVEL_ 字段数据为起购金额
import hashlib

from hbase.ttypes import BatchMutation, Mutation

from scripts import GenericScript


def data_shuffle(data):
    re_data = dict()

    # "C"
    # hash_m = hashlib.md5()
    # hash_m.update(data["NAME_"].encode("utf-8"))
    # hash_title = hash_m.hexdigest()
    re_data["ID_"] = "CEB" + "_" + data["CODE_"]
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # re_data["SOURCE_"]
    # re_data["AREA_CODE_"]
    re_data["BANK_CODE_"] = "CEB"
    re_data["BANK_NAME_"] = "交通银行"
    # re_data["UNIT_CODE_"]
    # re_data["PERIOD_CODE_"] = ""
    # 无 CONTENT_
    # re_data["CONTENT_"]
    re_data["STATUS_1"] = ""
    # re_data["REMARK_"] = ""
    re_data["CREATE_TIME_"] = data["DATE_TIME_"]
    # re_data["UPDATE_TIME_"]

    # "F"
    re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
    re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
    re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
    re_data["CODE_"] = data["CODE_"]
    re_data["START_FUNDS_"] = data["START_FUNDS_"]
    re_data["NAME_"] = data["NAME_"]
    re_data["URL_"] = data["URL_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["DATETIME_"] = data["DATETIME_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]

    return re_data


def run():
    script = GenericScript(entity_code="CEBFinancial", entity_type="FINPRODUCT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        re_data = data_shuffle(data)


if __name__ == '__main__':
    run()
