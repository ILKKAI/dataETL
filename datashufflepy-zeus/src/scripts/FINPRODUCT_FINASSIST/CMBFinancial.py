# -*- coding: utf-8 -*-

# 招商银行理财产品 CMBFinancial
# 无 CONTENT_  已完成
# 售卖时间范围  是否保本  可否赎回  私人银行  收益率（12-20前无）

import hashlib

from scripts import GenericScript


def data_shuffle(data):
    re_data = dict()
    # "C"
    # hash_m = hashlib.md5()
    # hash_m.update(data["NAME_"].encode("utf-8"))
    # hash_id = hash_m.hexdigest()
    re_data["ID_"] = "CMB" + "_" + data["CODE_"] + "_" + data["SALE_START_"]
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # re_data["AREA_CODE_"]
    re_data["BANK_CODE_"] = "CMB"
    re_data["BANK_NAME_"] = "招商银行"
    # re_data["UNIT_CODE_"]
    re_data["PERIOD_CODE_"] = data["SALE_START_"].replace("-", "")
    # re_data["CONTENT_"]
    re_data["STATUS_1"] = ""
    # re_data["REMARK_"]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    # re_data["UPDATE_TIME_"]

    # "F"
    re_data["CODE_"] = data["CODE_"]
    re_data["NAME_"] = data["NAME_"]
    # 售卖时间范围
    re_data["TIME_LIMIT_"] = data["SALE_START_"] + "_" + data["SALE_END_"]
    # 收益率
    if "YIELD_RATE_" in data:
        re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
    else:
        re_data["YIELD_RATE_"] = ""
    # 售卖区域
    if "SALE_AREA_" in data:
        re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
    else:
        re_data["SALE_DISTRICT_"] = ""
    # 起购金额
    if "START_FUNDS_" in data:
        re_data["START_FUNDS_"] = data["START_FUNDS_"]
    else:
        re_data["START_FUNDS_"] = ""
    # 期限
    re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
    # 开始售卖时间
    re_data["SALE_END_"] = data["SALE_END_"]
    # 结束售卖时间
    re_data["SALE_START_"] = data["SALE_START_"]
    # 风险等级
    re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
    re_data["URL_"] = data["URL_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["DATETIME_"] = data["DATETIME_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # 是否保本
    # re_data["BREAKEVEN_"] = data["BREAKEVEN_"]
    # 可否赎回
    # re_data["REDEMING_MODE_"]
    # 私人银行
    # re_data["PRIVATE_BANK_"]

    return re_data


def run():
    script = GenericScript(entity_code="CMBFinancial", entity_type="FINPRODUCT_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        re_data = data_shuffle(data)


if __name__ == '__main__':
    run()
