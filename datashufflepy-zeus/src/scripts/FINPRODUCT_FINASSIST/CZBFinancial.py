# -*- coding: utf-8 -*-

# 浙商银行 CZBFinancial
# 有 CONTENT_ 需清洗

# 两条 CONTENT_ 为空
# {$and: [{ ENTITY_CODE_: 'CZBFinancial'},{DATETIME_:{$gte:"2018-12-15"}}]}
import re

from scripts import GenericScript


def data_shuffle(data):
    data_list = list()
    if "CONTENT_" in data:
        if data["CONTENT_"]:
            print("*" * 200)
            print(data["CONTENT_"])
            print(data["URL_"])
            re_data = dict()

            first_shuffle = re.findall(r"(.*?)(固定期限理财产品.*)", data["CONTENT_"])
            print(first_shuffle)
            if first_shuffle:
                re_data_1 = dict()
                start_funds_1 = re.findall(r"\|\d\|\w元起", first_shuffle[0])
                risk_level_1 = "低风险"
                re_data_1["NAME_"] = "天天增金"
                re_data_1["CODE_"] = "815001"
                re_data_1["START_FUNDS_"] = start_funds_1[0]
                re_data_1["RISK_LEVEL_"] = risk_level_1[0]
                data_list.append(re_data_1)

                second_shuffle = re.findall(r"815001(.*?)[A-Z]{2}\d{4}")

                re_data_2 = dict()
                product_name = re.findall(r"(永乐\|\d\|号\|\d*\|天型)", data["CONTENT_"])
                product_code = re.findall(r"|永乐\|\d\|号\|\d*\|天型\|([^|]*)", data["CONTENT_"])
                if not product_name:
                    product_name = re.findall(r"(第\|\d\|期.*?理财产品)\|", data["CONTENT_"])
                    product_code = re.findall(r"第\|\d\|期.*?理财产品\|[^|]*", data["CONTENT_"])

                start_funds = re.findall(r"(\|\d*\|万元起)", data["CONTENT_"])
                yield_rate = re.findall(r"\d\.\d*%|详见公告", data["CONTENT_"])
                time_limit = re.findall(r"元\|(.*?)\|\w*风险", data["CONTENT_"])
                risk_level = re.findall(r"\|(\w*风险)\|", data["CONTENT_"])

                print(start_funds)
                print(yield_rate)
                print(time_limit)
                print(risk_level)
            return

            # "C"
            re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
            # re_data["AREA_CODE_"]
            re_data["BANK_CODE_"] = "CZB"
            re_data["BANK_NAME_"] = "浙商银行"
            # re_data["UNIT_CODE_"]
            re_data["PERIOD_CODE_"] = ""
            # re_data["CONTENT_"]
            re_data["STATUS_1"] = ""
            # re_data["REMARK_"]
            re_data["CREATE_TIME_"] = data["DATETIME_"]
            # re_data["UPDATE_TIME_"]

            # "F"
            re_data["CODE_"] = data["CODE_"]
            re_data["NAME_"] = data["NAME_"]
            # 售卖时间范围
            re_data["TIME_LIMIT_"] = data["TIME_LIMIT_"]
            # 收益率
            re_data["YIELD_RATE_"] = data["YIELD_RATE_"]
            # 售卖区域
            re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
            # 是否保本
            re_data["BREAKEVEN_"] = data["BREAKEVEN_"]
            # 起购金额
            re_data["START_FUNDS_"] = data["START_FUNDS_"]
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
            # 无
            # 可否赎回
            # re_data["REDEMING_MODE_"]
            # 私人银行
            # re_data["PRIVATE_BANK_"]

            return re_data
        else:
            return None
    else:
        return None


def run():
    script = GenericScript(entity_code="CZBFinancial", entity_type="FINPRODUCT_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        data_shuffle(data)


if __name__ == '__main__':
    run()
