# -*- coding: utf-8 -*-
"""中国银行理财产品 JRCP_LCCP_ZGYH_GW_ALL"""
import re

from database._mongodb import MongoClient
from tools.req_for_pdf import parse


def data_shuffle(data):
    # 产品名称
    data["PRO_NAME_"] = data["PDF_NAME_"]
    # 产品编码
    # pro_code = re.findall(r"(.*?)中银", data["PDF_NAME_"])
    pro_code = re.findall(r"([\da-zA-Z\-]{6,})", data["PDF_NAME_"])
    if pro_code:
        data["PRO_CODE_"] = pro_code[0]
    # 产品期限
    real_days = re.findall(r"(\d+)天", data["PDF_NAME_"])
    if real_days:
        data["REAL_DAYS_"] = real_days[0]
    # 募集起始日期
    raise_start = re.findall(r"\d{4}年\d{1,2}月\d{1,2}", data["PDF_NAME_"])
    if raise_start:
        start_date = re.sub(r"[\u4e00-\u9fa5]", "-", raise_start[0])
        data["RAISE_START_"] = start_date
    pdf_result = parse(pdf_url=data["PDF_"])
    # print(pdf_result)
    # 清洗 PDF
    # 风险等级
    risk_level = re.findall(r"(\w+风险)产品", pdf_result)
    if risk_level:
        data["SOURCE_RISK_LEVEL_"] = risk_level[0]
        if data["SOURCE_RISK_LEVEL_"] == "低风险":
            data["RISK_LEVEL_CODE_"] = "R1"
        elif data["SOURCE_RISK_LEVEL_"] == "中低风险":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["SOURCE_RISK_LEVEL_"] == "较低风险":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["SOURCE_RISK_LEVEL_"] == "中等风险":
            data["RISK_LEVEL_CODE_"] = "R3"
        elif data["SOURCE_RISK_LEVEL_"] == "中高风险":
            data["RISK_LEVEL_CODE_"] = "R4"
        elif data["SOURCE_RISK_LEVEL_"] == "高风险":
            data["RISK_LEVEL_CODE_"] = "R5"
    # 匹配表格中内容
    # 一、理财计划基本信息 二、理财计划投资范围、投资种类及比例  [投理][资财][对计][象划]及?投资[限范][制围]
    first_shuffle = re.findall(r"[一1]、 *理?财?[产计][品划]基本信息(.*)\n[二2]、", pdf_result, re.S)

    if first_shuffle:
        # 保证收益型   非保本浮动收益型  无固定期限非保本浮动收益型，实际产品期限受制于提前终止条款。
        # break_even = re.findall(r"产品类型 *\n([^\n]*)", first_shuffle[0])
        # break_even = re.findall(r"\n(\w*收益型)[；，]?\w*。? *\n", first_shuffle[0])

        # 售卖时间范围  理财计划认购期 【2018】年【11】月【21】日-【2018】年【11】月【22】日
        # time_limit = re.findall(r"[认募][购集]期 *\d{4} ?年?\d{1,2} ?月?\d{1,2} ?日?[-至] ?\d{4} ?年?\d{1,2} ?月?\d{1,2} ?日?", first_shuffle[0])
        time_limit = re.findall(
            r"\d{4}】? ?年?【?\d{1,2}】? ?月?【?\d{1,2}】? ?日?[-至] ?【?\d{4}】? ?年?【?\d{1,2}】? ?月?【?\d{1,2}】? ?日?",
            first_shuffle[0])
        if not time_limit:
            time_limit = re.findall(
                r"\d{4}】? ?年?【?\d{1,2}】? ?月?【?\d{1,2}】? ?日?[-至] ?【?\d{4}】? ?年?【?\d{1,2}】? ?月?【?\d{1,2}】? ?日?",
                first_shuffle[0])

        if time_limit:
            re_time = re.findall(r"\d{4} ?年 ?\d{1,2} ?月 ?\d{1,2} ?日", time_limit[0])
            if not re_time:
                re_time = re.findall(r"(【?\d{4}】? ?[年\\/-] ?【?\d{1,2}】? ?[月\\/-] ?【?\d{1,2}】? ?日?)", time_limit[0])
        # else:
        #     re_time = re.findall(r"(【?\d{4}】? ?[年\\/-] ?【?\d{1,2}】? ?[月\\/-] ?【?\d{1,2}】? ?日?)", first_shuffle[0])
            if re_time:
                if len(re_time) > 1:
                    # 结束售卖时间
                    raise_end = re.sub(r"[【】 ]", "", re_time[1])
                    data["RAISE_END_"] = re.sub(r"[年月]", "-", raise_end[:-1])

        # 期限
        inverst_period = re.findall(r"\n【?(\d*)】? ?天（?.*）? *\n", first_shuffle[0])
        if inverst_period:
            data["REAL_DAYS_"] = inverst_period[0]
        # 收益率
        yield_rate = re.findall(r"(【?\d\.\d{2}】?%) *\n", first_shuffle[0])
        if yield_rate:
            if len(yield_rate) == 1:
                data["YIELD_HIGH_"] = re.sub(r"[【】 %]", "", yield_rate[0])
                data["YIELD_LOW_"] = data["YIELD_HIGH_"]
            else:
                data["YIELD_LOW_"] = re.sub(r"[【】 %]", "", yield_rate[0])
                data["YIELD_HIGH_"] = re.sub(r"[【】 %]", "", yield_rate[1])

        # 起购金额
        funds = re.findall(r"起点([^\n]*)倍[递累][^\n]*", first_shuffle[0])
        if funds:
            re_funds = re.findall(r"\d+.*?元", funds[0])
            if re_funds:
                if len(re_funds) == 1:
                    funds = re.findall(r"认购起点金额.*?倍", first_shuffle[0], re.S)
                    if funds:
                        re_funds = re.findall(r"\d+.*?元", funds[0])
                        if re_funds:
                            if len(re_funds) == 2:
                                # data["START_FUNDS_"] = re_funds[0].replace(" ", "")
                                data["START_FUNDS_"] = re.sub(r"[^个十百千万亿元\d.]", "", re_funds[0])
                                data["INCREASE_"] = re.sub(r"[^个十百千万亿元\d.]", "", re_funds[0])
                            else:
                                pass
                else:
                    data["START_FUNDS_"] = re_funds[0].replace(" ", "")
                    data["INCREASE_"] = re_funds[1].replace(" ", "")
    else:
        pass
        # print(data["PDF_"])
        # print(pdf_result)

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
