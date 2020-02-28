# -*- coding: utf-8 -*-
import os
import re
import sys
import arrow


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-27])

from tools.req_for_api import req_for_serial_number
from branch_scripts2 import GenericScript
from tools.req_for_pdf import parse
from __config import TABLE_NAME


class BranchFinProduct(GenericScript):

    def __init__(self, table_name, collection_name, param, verify_field):
        super(BranchFinProduct, self).__init__(table_name, collection_name, param, verify_field=verify_field)

        self.opt_dict = {"封闭式非净值型": "FBSFJZ", "封闭式净值型": "FBSJZ", "开放式非净值型": "KFSFJZ", "开放式净值型": "KFSJZ"}

        self.invest_dict = {"混合类": "HHL", "债券类": "ZQL", "结构性投资类": "JGXTZL", "代客境外投资类": "DKJWTZL", "银行存款类": "YHCKL",
                            "拆放同业及买入返售类": "CFTYJMRFSL", "同业存单类": "TYCDL", "非标准化债权类": "FBZHZQL", "另类投资类": "LLTZL",
                            "货币市场工具类": "HBSCGJL", "理财直接融资工具类": "LCZJRZGJL", "股权类": "GQL", "股票类": "GPL"}
        self.risk_dict = {"R1": "低（R1）", "R2": "中低（R2）", "R3": "中（R3）", "R4": "中高（R4）", "R5": "高（R5）"}
        # if isinstance(self.entity_code, str):
        #     where_condition = f"ENTITY_CODE_ = '{self.entity_code}'"
        # elif isinstance(self.entity_code, list):
        #     if len(self.entity_code) == 1:
        #         where_condition = f"ENTITY_CODE_ = '{self.entity_code[0]}'"
        #     else:
        #         where_condition = f"ENTITY_CODE_ in {tuple(self.entity_code)}"
        # else:
        #     where_condition = None
        # self.verify_list = self.p_client.search_all_from_phoenix(connection=self.connection,
        #                                                          output_field=["PRO_NAME_", "PRO_CODE_"],
        #                                                          iter_status=False,
        #                                                          where_condition=where_condition)

    def __shuffle(self, data):
        re_data = dict()
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        re_data["URL_"] = data["URL_"]

        if "中国理财网" in data["ENTITY_NAME_"]:
            serial_number = req_for_serial_number(code="JRCP_LCCP_INFO")
            re_data["ID_"] = serial_number
            re_data["PRO_NAME_"] = data["PRO_NAME_"]
            re_data["PRO_ORG_"] = data["PRO_ORG_"]
            re_data["REGIST_CODE_"] = data["REGIST_CODE_"]
            re_data["PRO_STATUS_"] = data["PRO_STATUS_"]
            re_data["OPT_MODE_"] = data["OPT_MODE_"]
            try:
                re_data["OPT_MODE_CODE_"] = self.opt_dict[re_data["OPT_MODE_"]]
            except KeyError:
                re_data["OPT_MODE_CODE_"] = ""
            re_data["YIELD_TYPE_"] = data["YIELD_TYPE_"]
            # re_data["YIELD_TYPE_CODE_"] = data[""]
            re_data["CURRENCY_TYPE_"] = data["CURRENCY_TYPE_"]
            # re_data["CURRENCY_TYPE_CODE_"] = data[""]
            re_data["START_FUNDS_"] = data["START_FUNDS_"]
            try:
                if float(data["START_FUNDS_"]) <= 10000:
                    re_data["START_FUNDS_CODE_"] = "S0_1"
                elif 10000 < float(data["START_FUNDS_"]) <= 50000:
                    re_data["START_FUNDS_CODE_"] = "S1_5"
                elif 50000 < float(data["START_FUNDS_"]) < 100000:
                    re_data["START_FUNDS_CODE_"] = "S5_10"
                elif 100000 < float(data["START_FUNDS_"]):
                    re_data["START_FUNDS_CODE_"] = "S10_"
            except Exception:
                re_data["START_FUNDS_"] = 0

            re_data["SOURCE_RISK_LEVEL_"] = data["SOURCE_RISK_LEVEL_"]
            if "SOURCE_RISK_LEVEL_" in data:
                if data["SOURCE_RISK_LEVEL_"][0] == "一":
                    re_data["RISK_LEVEL_CODE_"] = "R1"
                    re_data["RISK_LEVEL_"] = self.risk_dict[re_data["RISK_LEVEL_CODE_"]]
                elif data["SOURCE_RISK_LEVEL_"][0] == "二":
                    re_data["RISK_LEVEL_CODE_"] = "R2"
                    re_data["RISK_LEVEL_"] = self.risk_dict[re_data["RISK_LEVEL_CODE_"]]
                elif data["SOURCE_RISK_LEVEL_"][0] == "三":
                    re_data["RISK_LEVEL_CODE_"] = "R3"
                    re_data["RISK_LEVEL_"] = self.risk_dict[re_data["RISK_LEVEL_CODE_"]]
                elif data["SOURCE_RISK_LEVEL_"][0] == "四":
                    re_data["RISK_LEVEL_CODE_"] = "R4"
                    re_data["RISK_LEVEL_"] = self.risk_dict[re_data["RISK_LEVEL_CODE_"]]
                elif data["SOURCE_RISK_LEVEL_"][0] == "五":
                    re_data["RISK_LEVEL_CODE_"] = "R5"
                    re_data["RISK_LEVEL_"] = self.risk_dict[re_data["RISK_LEVEL_CODE_"]]

            re_data["RAISE_START_"] = data["RAISE_START_"]
            re_data["RAISE_END_"] = data["RAISE_END_"]
            re_data["PRO_START_"] = data["PRO_START_"]
            re_data["PRO_END_"] = data["PRO_END_"]
            re_data["YIELD_LOW_"] = data["YIELD_LOW_"]
            re_data["YIELD_HIGH_"] = data["YIELD_HIGH_"]
            try:
                if float(re_data["YIELD_HIGH_"]) < 3.5:
                    re_data["YIELD_HIGH_CODE_"] = "Y1_3.5"
                if 3.5 < float(re_data["YIELD_HIGH_"]) <= 4.5:
                    re_data["YIELD_HIGH_CODE_"] = "Y3.5_4.5"
                if 4.5 < float(re_data["YIELD_HIGH_"]):
                    re_data["YIELD_HIGH_CODE_"] = "Y4.5_"
            except Exception:
                re_data["YIELD_LOW_"] = 0
                re_data["YIELD_HIGH_"] = 0

            re_data["REAL_DAYS_"] = data["REAL_DAYS_"]
            if re_data["REAL_DAYS_"]:
                try:
                    if int(re_data["REAL_DAYS_"]) <= 90:
                        re_data["REAL_DAYS_CODE_"] = "D1_3"
                    elif 90 < int(re_data["REAL_DAYS_"]) < 180:
                        re_data["REAL_DAYS_CODE_"] = "D3_6"
                    elif 180 < int(re_data["REAL_DAYS_"]) <= 366:
                        re_data["REAL_DAYS_CODE_"] = "D6_12"
                    elif 366 < int(re_data["REAL_DAYS_"]):
                        re_data["REAL_DAYS_CODE_"] = "D12_"
                except Exception:
                    re_data["REAL_DAYS_"] = 0
            re_data["INVEST_TYPE_"] = data["INVEST_TYPE_"]
            try:
                re_data["INVEST_TYPE_CODE_"] = self.invest_dict[re_data["INVEST_TYPE_"]]
            except KeyError:
                re_data["INVEST_TYPE_CODE_"] = ""

            re_data["TARGET_TYPE_"] = data["TARGET_TYPE_"]
            re_data["SALE_AREA_"] = data["SALE_AREA_"]
            if "PROVINCE_NAME_" in data:
                re_data["PROVINCE_NAME_"] = data["PROVINCE_NAME_"]
            if "PROVINCE_NAME_" in data:
                re_data["PROVINCE_CODE_"] = data["PROVINCE_CODE_"]
            if "CITY_NAME_" in data:
                re_data["CITY_NAME_"] = data["CITY_NAME_"]
            if "CITY_CODE_" in data:
                re_data["CITY_CODE_"] = data["CITY_CODE_"]

            # re_data["REDEEM_"] = data[""]
            # re_data["INCREASE_"] = data[""]
            # re_data["INVEST_RANGE_"] = data[""]
            re_data["VERSION_"] = "0"

            bank_list = list()
            bank_code_list = list()
            for each in self.bank_list:
                if each["NAME_"] in data.get("ENTITY_NAME_", ""):
                    bank_list.append(each["NAME_"])
                    bank_code_list.append(each["CODE_"])
            if bank_list:
                re_data["BANK_NAME_"] = "|".join(bank_list)
            if bank_code_list:
                re_data["BANK_CODE_"] = "|".join(bank_code_list)

            re_data = super(BranchFinProduct, self).generic_shuffle(data=data, re_data=re_data, field=None)

            del re_data["CREATE_TIME_"]
            del re_data["SPIDER_TIME_"]
            del re_data["M_STATUS_"]
            del re_data["DELETE_STATUS_"]
            del re_data["DATA_STATUS_"]
            del re_data["PUBLISH_STATUS_"]
            return {"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_FINANCIAL_INFO"), "DATA_": re_data}
        else:
            source = re.findall(r"(https?://.*?)/", data["URL_"])
            re_data["SOURCE_"] = source[0]
            re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"]
            serial_number = req_for_serial_number(code="JRCP_LCCP")
            re_data["ID_"] = serial_number
            re_data["SOURCE_TYPE_"] = ""
            # if "PRO_NAME_" not in data:
            #     return
            re_data["PRO_NAME_"] = data["PRO_NAME_"]
            f_index = data["ENTITY_NAME_"].find("-")
            re_data["PRO_ORG_"] = data["ENTITY_NAME_"][:f_index]
            if "PRO_CODE_" in data:
                re_data["PRO_CODE_"] = data["PRO_CODE_"]
            # 登记编码
            if "REGIST_CODE_" in data:
                re_data["REGIST_CODE_"] = data["REGIST_CODE_"]
            else:
                if "PDF_" in data:
                    try:
                        text = parse(data["PDF_"])
                        registration_code = re.findall(r"C\d{13}", text)
                        if registration_code:
                            re_data["REGIST_CODE_"] = registration_code[0]
                    except Exception as e:
                        self.logger.exception(f"2.1--err: PDF."
                                              f" 原始数据 collection = {self.m_client.mongo_collection};"
                                              f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                              f" 原始数据 _id = {data['_id']};"
                                              f" error: {e}.")
            # 预售(PRE)、在售(ON)、停售(STOP)
            # 全部为 在售
            re_data["PRO_STATUS_"] = "ON"
            if "OPT_MODE_" in data:
                re_data["OPT_MODE_"] = data["OPT_MODE_"]
                try:
                    re_data["OPT_MODE_CODE_"] = self.opt_dict[re_data["OPT_MODE_"]]
                except KeyError:
                    re_data["OPT_MODE_CODE_"] = ""

            if "YIELD_TYPE_" in data:
                re_data["YIELD_TYPE_"] = data["YIELD_TYPE_"]
                # re_data["YIELD_TYPE_CODE_"] = data[""]
            if "CURRENCY_TYPE_" in data:
                re_data["CURRENCY_TYPE_"] = data["CURRENCY_TYPE_"]
            # re_data["CURRENCY_TYPE_CODE_"] = data[""]
            # 起购金额
            if "START_FUNDS_" in data:
                start_funds = data["START_FUNDS_"].replace(" ", "")
                start_funds = start_funds.replace("亿", "00000000")
                start_funds = start_funds.replace("千万", "0000000")
                start_funds = start_funds.replace("百万", "000000")
                start_funds = start_funds.replace("十万", "00000")
                start_funds = start_funds.replace("万", "0000")
                start_funds = start_funds.replace("千", "000")
                start_funds = start_funds.replace("百", "00")
                start_funds = start_funds.replace("元", "")

                re_data["START_FUNDS_"] = start_funds

                try:
                    if float(re_data["START_FUNDS_"]) <= 10000:
                        re_data["START_FUNDS_CODE_"] = "S0_1"
                    elif 10000 < float(re_data["START_FUNDS_"]) <= 50000:
                        re_data["START_FUNDS_CODE_"] = "S1_5"
                    elif 50000 < float(re_data["START_FUNDS_"]) <= 100000:
                        re_data["START_FUNDS_CODE_"] = "S5_10"
                    elif 100000 < float(re_data["START_FUNDS_"]):
                        re_data["START_FUNDS_CODE_"] = "S10_"
                except Exception as e:
                    re_data["START_FUNDS_"] = 0

            if "RISK_LEVEL_CODE_" in data:
                re_data["RISK_LEVEL_"] = self.risk_dict[data["RISK_LEVEL_CODE_"]]
                re_data["RISK_LEVEL_CODE_"] = data["RISK_LEVEL_CODE_"]

            if "RISK_LEVEL_" in data:
                re_data["SOURCE_RISK_LEVEL_"] = data["RISK_LEVEL_"]
            elif "SOURCE_RISK_LEVEL_" in data:
                re_data["SOURCE_RISK_LEVEL_"] = data["SOURCE_RISK_LEVEL_"]
            # # 募集起始日期
            if "RAISE_START_" in data:
                re_data["RAISE_START_"] = data["RAISE_START_"]
            # # 募集结束日期
            if "RAISE_END_" in data:
                re_data["RAISE_END_"] = data["RAISE_END_"]
            # # 产品起始日期
            if "PRO_START_" in data:
                re_data["PRO_START_"] = data["PRO_START_"]
            # # 产品结束日期
            if "PRO_END_" in data:
                re_data["PRO_END_"] = data["PRO_END_"]
            # 预期最低收益率
            if "YIELD_LOW_" in data:
                re_data["YIELD_LOW_"] = data["YIELD_LOW_"].replace("%", "")
            # 预期最高收益率
            if "YIELD_HIGH_" in data:
                re_data["YIELD_HIGH_"] = data["YIELD_HIGH_"].replace("%", "")
                if re_data["YIELD_HIGH_"]:
                    try:
                        if float(re_data["YIELD_HIGH_"]) <= 3.5:
                            re_data["YIELD_HIGH_CODE_"] = "Y1_3.5"
                        elif 3.5 < float(re_data["YIELD_HIGH_"]) <= 4.5:
                            re_data["YIELD_HIGH_CODE_"] = "Y3.5_4.5"
                        elif 4.5 < float(re_data["YIELD_HIGH_"]):
                            re_data["YIELD_HIGH_CODE_"] = "Y4.5_"
                    except Exception:
                        re_data["YIELD_HIGH_"] = 0
                        re_data["YIELD_LOW_"] = 0
            # 实际天数
            if "REAL_DAYS_" in data:
                data["REAL_DAYS_"] = data["REAL_DAYS_"].replace(" ", "")
                if "年" in data["REAL_DAYS_"]:
                    re_data["REAL_DAYS_"] = data["REAL_DAYS_"].replace("年", "")
                    try:
                        re_data["REAL_DAYS_"] = int(re_data["REAL_DAYS_"]) * 365
                    except Exception:
                        re_data["REAL_DAYS_"] = 0
                elif "月" in data:
                    re_data["REAL_DAYS_"] = data["REAL_DAYS_"].replace("月", "")
                    try:
                        re_data["REAL_DAYS_"] = int(re_data["REAL_DAYS_"]) * 30
                    except Exception:
                        re_data["REAL_DAYS_"] = 0
                else:
                    re_data["REAL_DAYS_"] = data["REAL_DAYS_"].replace("天", "")
                try:
                    if int(re_data["REAL_DAYS_"]) <= 90:
                        re_data["REAL_DAYS_CODE_"] = "D1_3"
                    elif 90 < int(re_data["REAL_DAYS_"]) < 180:
                        re_data["REAL_DAYS_CODE_"] = "D3_6"
                    elif 180 < int(re_data["REAL_DAYS_"]) <= 366:
                        re_data["REAL_DAYS_CODE_"] = "D6_12"
                    elif 366 < int(re_data["REAL_DAYS_"]):
                        re_data["REAL_DAYS_CODE_"] = "D12_"
                except Exception:
                    re_data["REAL_DAYS_"] = 0
            else:
                if "PRO_START_" in data and "PRO_END_" in data:
                    t_start = arrow.get(data["PRO_START_"], "YYY-MM-DD")
                    t_end = arrow.get(data["PRO_END_"], "YYYY-MM-DD")
                    real_days = t_end - t_start
                    data["REAL_DAYS_"] = real_days.days

            if "INVEST_TYPE_" in data:
                re_data["INVEST_TYPE_"] = data["INVEST_TYPE_"]
                try:
                    re_data["INVEST_TYPE_CODE_"] = self.invest_dict[re_data["INVEST_TYPE_"]]
                except KeyError:
                    re_data["INVEST_TYPE_CODE_"] = ""

            # # 投资者类型
            if "TARGET_TYPE_" in data:
                re_data["TARGET_TYPE_"] = data["TARGET_TYPE_"]
            if "SALE_AREA_" in data:
                re_data["SALE_AREA_"] = data["SALE_AREA_"]
            # # 可否赎回
            if "REDEEM_" in data:
                if "不" in data["REDEEM_"]:
                    re_data["REDEEM_"] = "N"
                else:
                    re_data['REDEEM_'] = "Y"
            if "INCREASE_" in data:
                increase = data["INCREASE_"].replace(" ", "")
                increase = increase.replace("亿", "00000000")
                increase = increase.replace("千万", "0000000")
                increase = increase.replace("百万", "000000")
                increase = increase.replace("十万", "00000")
                increase = increase.replace("万", "0000")
                increase = increase.replace("千", "000")
                increase = increase.replace("百", "00")
                increase = increase.replace("元", "")
                re_data["INCREASE_"] = increase
                # re_data["INVEST_RANGE_"] = data["INVEST_RANGE_"]
            re_data["RECOMMEND_"] = "N"
            re_data["GOOD_SALE_"] = "N"
            re_data["NEW_SALE_"] = "N"
            re_data["SALE_SOURCE_"] = "NET"

            bank_list = list()
            bank_code_list = list()
            for each in self.bank_list:
                if each["NAME_"] in data.get("ENTITY_NAME_", ""):
                    bank_list.append(each["NAME_"])
                    bank_code_list.append(each["CODE_"])
            if bank_list:
                re_data["BANK_NAME_"] = "|".join(bank_list)
            if bank_code_list:
                re_data["BANK_CODE_"] = "|".join(bank_code_list)

            re_data = super(BranchFinProduct, self).generic_shuffle(data=data, re_data=re_data, field=None)
            re_data["PUBLISH_TIME_"] = re_data["SPIDER_TIME_"]
            return {"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_FINANCIAL_PRODUCT"), "DATA_": re_data}

    def generic_shuffle(self, data, field="PRO_NAME_"):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        re_list = list()
        if isinstance(data, dict):
            if "PRO_NAME_" in data:
                pro_name = data["PRO_NAME_"]
            else:
                pro_name = None
            if "PRO_CODE_" in data:
                pro_code = data["PRO_CODE_"]
            else:
                pro_code = None
            # if (pro_name, pro_code) in self.verify_list:
            #     self.logger.info(f"重复值: {(pro_name, pro_code)}")
            #     return
            re_data = self.__shuffle(data)
            re_list.append(re_data)
        else:
            for each in data:
                # if "PRO_NAME_" in each:
                #     pro_name = each["PRO_NAME_"]
                # else:
                #     pro_name = None
                # if "PRO_CODE_" in each:
                #     pro_code = each["PRO_CODE_"]
                # else:
                #     pro_code = None
                # if (pro_name, pro_code) in self.verify_list:
                #     self.logger.info(f"重复值: {(pro_name, pro_code)}")
                #     continue
                # else:
                re_data = self.__shuffle(each)
                re_list.append(re_data)
        return re_list


if __name__ == '__main__':
    # param = sys.argv[1]
    param = "{'entityType':'JRCP_LCCP','limitNumber':1000,'entityCode':['JRCP_LCCP_HEBYH_GW_ALL']}"

    if "ZGLCW_ALL" in param:
        table_name = "CHA_BRANCH_FINANCIAL_INFO"
        verify_field = {"PRO_NAME_": "PRO_NAME_", "REGIST_CODE_": "REGIST_CODE_"}
    else:
        table_name = "CHA_BRANCH_FINANCIAL_PRODUCT"
        verify_field = {"PRO_NAME_": "PRO_NAME_", "PRO_CODE_": "PRO_CODE_"}
    script = BranchFinProduct(table_name=TABLE_NAME(table_name), collection_name="JRCP_LCCP", param=param, verify_field=verify_field)
    script.main()
    script.close_client()
