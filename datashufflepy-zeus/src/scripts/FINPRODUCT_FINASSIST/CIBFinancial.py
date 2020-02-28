# -*- coding: utf-8 -*-

# 兴业银行理财产品 CIBFinancial

# CONTENT_ 已完成
import hashlib
import re

import arrow

from scripts import GenericScript


def data_shuffle(data):
    data_list = list()
    if "CONTENT_" in data:
        # 产品编码
        product_code = re.findall(r"\|\d{4}年第\d*期[^|]*\|(\d{8})\|", data["CONTENT_"])
        # product_login = re.findall(r"\|(C103\d*)\|", data["CONTENT_"])
        # 产品名称
        product_first_name = re.findall(r"兴业银行(.*?)要素表", data["CONTENT_"])
        product_name = re.findall(r"\|(\d{4}年第\d*期[^|]*)\|", data["CONTENT_"])
        # 认购起始日
        sale_start = re.findall(r"C\d*\|([^|]*)", data["CONTENT_"])
        # 认购结束日
        sale_end = re.findall(r"C\d*\|[^|]*\|([^|]*)", data["CONTENT_"])
        # 期限
        invest_period = re.findall(r"\|(\d{1,3})\|非?保本浮动收益|\|(\d{1,3})\|净值型", data["CONTENT_"])
        # 是否保本
        break_even = re.findall(r"(非?保本浮动收益)|(净值型)", data["CONTENT_"])
        # 起购金额
        start_funds = re.findall(r"\|(\d*[美]?[万元][^|]*)\|", data["CONTENT_"])
        # 收益率
        yield_rate = re.findall(r"\|\d*[美]?[万元][^|]*\|([^|]*)", data["CONTENT_"])

        for each in range(len(product_code)):
            re_data = dict()
            product_code_e = product_code[each]
            product_name_e = product_first_name[0] + product_name[each]
            sale_start_e = sale_start[each]
            if "/" in sale_start_e:
                time_split = sale_start_e.split("/")
                year, month, day = time_split
                t = arrow.Arrow(year=int(year), month=int(month), day=int(day))
                sale_start_e = t.format("YYYY-MM-DD")
            elif "-" in sale_start_e:
                pass
            else:
                year = sale_start_e[:4]
                month = sale_start_e[4:6]
                day = sale_start_e[6:]
                t = arrow.Arrow(year=int(year), month=int(month), day=int(day))
                sale_start_e = t.format("YYYY-MM-DD")
            sale_end_e = sale_end[each]
            if "/" in sale_end_e:
                time_split = sale_end_e.split("/")
                year, month, day = time_split
                t = arrow.Arrow(year=int(year), month=int(month), day=int(day))
                sale_end_e = t.format("YYYY-MM-DD")
            elif "-" in sale_end_e:
                pass
            else:
                year = sale_end_e[:4]
                month = sale_end_e[4:6]
                day = sale_end_e[6:]
                t = arrow.Arrow(year=int(year), month=int(month), day=int(day))
                sale_end_e = t.format("YYYY-MM-DD")

            i, j = invest_period[each]
            if i:
                invest_period_e = i
            else:
                invest_period_e = j
            m, n = break_even[each]
            if m:
                break_even_e = m
            else:
                break_even_e = n
            start_funds_e = start_funds[each]
            yield_rate_e = yield_rate[each]

            # "C"
            # hash_m = hashlib.md5()
            # hash_m.update(product_name_e.encode("utf-8"))
            # hash_id = hash_m.hexdigest()
            re_data["ID_"] = "CIB" + "_" + product_code_e + "_" + str(sale_start_e)
            re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
            # re_data["AREA_CODE_"]
            re_data["BANK_CODE_"] = "CIB"
            re_data["BANK_NAME_"] = "兴业银行"
            # re_data["UNIT_CODE_"]
            re_data["PERIOD_CODE_"] = str(sale_start_e).replace("-", "")
            re_data["CONTENT_"] = data["CONTENT_"]
            re_data["STATUS_1"] = ""
            # re_data["REMARK_"]
            re_data["CREATE_TIME_"] = data["DATETIME_"]
            # re_data["UPDATE_TIME_"]

            # "F"
            re_data["CODE_"] = product_code_e
            re_data["NAME_"] = product_name_e
            # 售卖时间范围
            re_data["TIME_LIMIT_"] = str(sale_start_e) + "-" + str(sale_end_e)
            # 收益率
            re_data["YIELD_RATE_"] = yield_rate_e
            # 售卖区域
            # re_data["SALE_DISTRICT_"] = ""
            # 是否保本
            re_data["BREAKEVEN_"] = break_even_e
            # 起购金额
            re_data["START_FUNDS_"] = start_funds_e
            # 期限
            re_data["INVEST_PERIOD_"] = invest_period_e
            # 开始售卖时间
            re_data["SALE_START_"] = str(sale_start_e)
            # 结束售卖时间
            re_data["SALE_END_"] = str(sale_end_e)
            # 风险等级
            # re_data["RISK_LEVEL_"] = ""
            re_data["URL_"] = data["URL_"]
            re_data["DEALTIME_"] = data["DEALTIME_"]
            re_data["DATETIME_"] = data["DATETIME_"]
            re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
            # 无
            # # 可否赎回
            # re_data["REDEMING_MODE_"]
            # # 私人银行
            # re_data["PRIVATE_BANK_"]

            data_list.append(re_data)
        return data_list
    else:
        return None


def run():
    script = GenericScript(entity_code="CIBFinancial", entity_type="FINPRODUCT_FINASSIST")

    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        re_data = data_shuffle(data)


if __name__ == '__main__':
    run()
