# -*- coding: utf-8 -*-
import hashlib

# BANK_NAME_ 字典
import re

NAME_DICT = {"ICBC": "中国工商银行", "ABC": "中国农业银行", "BOC": "中国银行", "CCB": "中国建设银行",
             "BOCOM": "交通银行", "PSBC": "中国邮政储蓄银行", "CZB": "浙商银行", "CBHB": "渤海银行",
             "ECITIC": "中信银行", "CEB": "中国光大银行", "HXB": "华夏银行", "CMBC": "中国民生银行",
             "CMB": "招商银行", "CIB": "兴业银行", "CGB": "广发银行", "PAB": "平安银行",
             "SPDB": "浦发银行", "EBCL": "恒丰银行"}


def data_shuffle(data, sales_status, produc_category, revenue_type, operaton_pattern, purchase_amount, duration_type):
    re_data = dict()

    # HBase row_key
    hash_m = hashlib.md5()
    hash_m.update(data["NAME_"].encode("utf-8"))
    hash_id = hash_m.hexdigest()

    re_data["ID_"] = data["ENTITY_CODE_"] + "_" + hash_id
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # re_data["AREA_CODE_"]

    for key, value in NAME_DICT.items():
        if value in data["PRODSOURCE_"]:
            re_data["BANK_CODE_"] = key
            re_data["BANK_NAME_"] = value

    # re_data["UNIT_CODE_"]
    re_data["PERIOD_CODE_"] = data["SALE_START_"].replace("/", "")
    # re_data["REMARK_"]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    # re_data["UPDATE_TIME_"]
    re_data["STATUS_"] = "1"
    re_data["CODE_"] = data["CODE_"]
    re_data["NAME_"] = data["NAME_"]
    # re_data["TIME_LIMIT_"]
    # if data["LOW_YIELD_RATE_"] and data["HIGH_YIELD_RATE_"]:
    #     re_data["YIELD_RATE_"] = data["LOW_YIELD_RATE_"] + "-" + data["HIGH_YIELD_RATE_"]
    # elif data["LOW_YIELD_RATE_"] and (not data["HIGH_YIELD_RATE_"]):
    #     re_data["YIELD_RATE_"] = data["LOW_YIELD_RATE_"]
    # elif (not data["LOW_YIELD_RATE_"]) and data["HIGH_YIELD_RATE_"]:
    #     re_data["YIELD_RATE_"] = data["HIGH_YIELD_RATE_"]
    re_data["LOW_YIELD_RATE_"] = data["LOW_YIELD_RATE_"].replace("%", "")
    re_data["HIGH_YIELD_RATE_"] = data["HIGH_YIELD_RATE_"].replace("%", "")
    # print(self.produc_category)

    for i in sales_status:
        if i["ITEM_LABEL_"] in data["STATUS_"]:
            re_data["SALE_STATUS_"] = data["STATUS_"]
            re_data["SALE_STATUS_"] = i["ITEM_VALUE_"]
            break

    for i in produc_category:
        if i["ITEM_LABEL_"] in data["INVESTOR_TYPE_"]:
            re_data["INVESTOR_TYPE_"] = data["INVESTOR_TYPE_"]
            re_data["INVESTOR_TYPE_CODE_"] = i["ITEM_VALUE_"]
            break

    # 起购金额
    if data["START_FUNDS_"]:
        if int(data["START_FUNDS_"]) < 50000:
            match_funds = "5万以下"
        elif 50000 <= int(data["START_FUNDS_"]) < 100000:
            match_funds = "5万-10万"
        elif 100000 <= int(data["START_FUNDS_"]) < 200000:
            match_funds = "10万-20万"
        elif 20000 <= int(data["START_FUNDS_"]) < 500000:
            match_funds = "20万-50万"
        elif 500000 <= int(data["START_FUNDS_"]):
            match_funds = "50万以上"
    else:
        match_funds = "不限"
    for i in purchase_amount:
        if match_funds in i["ITEM_LABEL_"]:
            re_data["START_FUNDS_"] = data["START_FUNDS_"]
            re_data["START_FUNDS_CODE_"] = i["ITEM_VALUE_"]

    # 期限
    if data["INVEST_PERIOD_"]:
        if int(data["INVEST_PERIOD_"]) <= 30:
            match_str = "1个月内"
        elif 30 < int(data["INVEST_PERIOD_"]) <= 90:
            match_str = "1-3个月（含）"
        elif 90 < int(data["INVEST_PERIOD_"]) <= 180:
            match_str = "3-6个月（含）"
        elif 180 < int(data["INVEST_PERIOD_"]) <= 365:
            match_str = "6-12个月（含）"
        elif 365 < int(data["INVEST_PERIOD_"]):
            match_str = "12个月以上"

    else:
        match_str = "不限"

    for i in duration_type:
        if match_str in i["ITEM_LABEL_"]:
            re_data["INVEST_PERIOD_"] = data["INVEST_PERIOD_"]
            re_data["INVEST_PERIOD_CODE_"] = i["ITEM_VALUE_"]

    if "SALE_AREA_" in data:
        re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
    re_data["SALE_START_"] = data["SALE_START_"].replace("/", "-")
    re_data["SALE_END_"] = data["SALE_END_"].replace("/", "-")
    re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
    # re_data["REDEMING_MODE_"]
    # re_data["PRIVATE_BANK_"]
    if "URL_" in data:
        re_data["URL_"] = data["URL_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["DATETIME_"] = data["DATETIME_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # re_data["CURRENCY_TYPE_"]
    # re_data["INCREASE_UNIT_"]
    re_data["YIELD_START_DATE_"] = data["PRO_START_"]
    re_data["YIELD_END_DATE_"] = data["PRO_END_"]

    for i in revenue_type:
        if i["ITEM_LABEL_"] == data["BREAKEVEN_"]:
            re_data["YIELD_TYPE_"] = data["BREAKEVEN_"]
            re_data["YIELD_TYPE_CODE_"] = i["ITEM_VALUE_"]
            break

    # re_data["TARGET_"]
    # re_data["PRODUCT_TYPE_"]
    for i in operaton_pattern:
        if i["ITEM_LABEL_"] in data["OPERA_MODEL_"]:
            re_data["OPERA_MODEL_"] = data["OPERA_MODEL_"]
            re_data["OPERA_MODEL_CODE_"] = i["ITEM_VALUE_"]
            break

    # re_data["INVEST_RANGE_"]
    # re_data["PRE_STOP_"]
    # re_data["RASE_PLAN_"]
    # re_data["PURCHASE_"]
    # re_data["CONTENT_"]

    re_data["NAME_"] = re.sub(r"[Xx*]*[年]", re_data["YIELD_START_DATE_"][:4]+"年", re_data["NAME_"])
    re_data["NAME_"] = re.sub(r"[Xx*]*[天]", re_data["INVEST_PERIOD_"]+"天", re_data["NAME_"])

    return re_data
