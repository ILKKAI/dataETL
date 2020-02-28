# -*- coding: utf-8 -*-

# 渤海银行 CBHBFinancial
# 有 CONTENT_ 需清洗  PDF  已完成

# 售卖时间范围  售卖区域  起购金额  开始售卖时间  结束售卖时间  风险等级  可否赎回  私人银行

import hashlib
import re

from scripts import GenericScript


def data_shuffle(data):
    re_data = dict()
    data["CONTENT_"] = data["CONTENT_"].replace(" ", "")
    product_name = re.findall(r"产品名称[：:](\w*\n?\w*[(（]?\w*\n?[)）]?\w*\n?\w*产品)", data["CONTENT_"])
    if product_name:
        product_name = product_name[0].replace("\n", "")
    product_code = re.findall(r"产品登记编码[：:](\w*)", data["CONTENT_"])
    if product_code:
        product_code = product_code[0]
    product_type = re.findall(r"产品类型[：:](\w*)", data["CONTENT_"])
    if product_type:
        product_type = product_type[0]
    start_time = re.findall(r"于(\w*)成立", data["CONTENT_"])
    if start_time:
        start_time = start_time[0]
    end_time = re.findall(r"产品到期日[：:](\w*)", data["CONTENT_"])
    if end_time:
        end_time = end_time[0]
    time_limit = ""
    invest_period = re.findall(r"期限[：:]([\w:：%；;、，,.。]*)", data["CONTENT_"])
    if invest_period:
        invest_period = invest_period[0]
    yield_rate = re.findall(r"[业收][绩益].*[:：] ?(\d*\.\d*-?\d*\.?\d*)", data["CONTENT_"])
    if yield_rate:
        yield_rate = yield_rate[0]
    else:
        yield_rate = ""

    if not product_name:
        return None
        # print(data["CONTENT_"])
        # product_name = re.findall(r"([\"“]\w*[\"”]产品)", data["CONTENT_"])
        # if product_name:
        #     product_name = product_name[0]
        # product_code = ""
        # product_type = ""
        # end_time = ""
        # time_limit = re.findall(r"\d*-\d*-\d*.*", data["CONTENT_"])
        # if time_limit:
        #     print(time_limit, "*"*150)
        #     return None
        #     limit_dict = dict()
        #     for limit in time_limit:
        #         limit_dict[time_limit.index(limit)] = limit
        #         # todo several start time and end time
        # # start_time = time_limit.split("至")
        # invest_period = re.findall(r"[^-]\d*\.\d*", data["CONTENT_"])
        # if invest_period:
        #     invest_period = invest_period[0]
        # # yield_rate = re.findall(r"")
    product_name = product_name.replace("\n", "")

    # "C"
    # hash_m = hashlib.md5()
    # hash_m.update(data["NAME_"].encode("utf-8"))
    # hash_id = hash_m.hexdigest()
    re_data["ID_"] = "CBHB" + "_" + product_code
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # re_data["AREA_CODE_"]
    re_data["BANK_CODE_"] = "CBHB"
    re_data["BANK_NAME_"] = "渤海银行"
    # re_data["UNIT_CODE_"]
    re_data["PERIOD_CODE_"] = ""
    re_data["CONTENT_"] = data["CONTENT_"]
    re_data["STATUS_1"] = ""
    # re_data["REMARK_"]
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    # re_data["UPDATE_TIME_"]

    # "F"
    re_data["CODE_"] = product_code
    re_data["NAME_"] = product_name
    # 售卖时间范围
    re_data["TIME_LIMIT_"] = time_limit
    # 收益率
    re_data["YIELD_RATE_"] = yield_rate
    # 售卖区域
    # re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
    # 是否保本
    re_data["BREAKEVEN_"] = data["BREAKEVEN_"]
    # 起购金额
    # re_data["START_FUNDS_"] = data["START_FUNDS_"]
    # 期限
    re_data["INVEST_PERIOD_"] = invest_period
    # 开始售卖时间
    # re_data["SALE_END_"] = data["SALE_END_"]
    # 结束售卖时间
    # re_data["SALE_START_"] = data["SALE_START_"]
    # 风险等级
    # re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
    re_data["URL_"] = data["URL_"]
    re_data["DEALTIME_"] = data["DEALTIME_"]
    re_data["DATETIME_"] = data["DATETIME_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # 无
    # 可否赎回
    # re_data["REDEMING_MODE_"]
    # 私人银行
    # re_data["PRIVATE_BANK_"]
    # with open("C:/Users/kevin/Desktop/financial/渤海银行_CBHB.", "a", encoding="utf-8") as f:
    #     f.write(str(re_data))
    #     f.write("\n\n")
    return re_data


def run():
    script = GenericScript(entity_code="CBHBFinancial", entity_type="FINPRODUCT_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        data_shuffle(data)


if __name__ == '__main__':
    run()
