# -*- coding: utf-8 -*-
"""浙商银行 理财产品 JRCP_LCCP_ZSYH_GW_ALL1"""
import re

from database._mongodb import MongoClient
from tools.req_for_pdf import parse


def data_shuffle(data):
    result = parse(data["PDF_"])
    # 产品名称
    pro_name = re.findall(r"(.*理财产品)说明书", result)
    if pro_name:
        data["PRO_NAME_"] = pro_name[0]
    # 发行机构
    data["PRO_ORG_"] = "浙商银行股份有限公司"
    # 登记编码
    regist_code = re.findall(r"C\d{13}", result)
    if regist_code:
        data["REGIST_CODE_"] = regist_code[0]
    # 运作模式
    # opt_mode = re.findall(r"非保本浮动收益型 ")
    # data["OPT_MODE_"] =
    # # 收益类型
    # data["YIELD_TYPE_"] =
    # 募集币种
    currency_type = re.findall(r"([人美][民元]币?) \n", result)
    if currency_type:
        data["CURRENCY_TYPE_"] = currency_type[0]
    # 起购金额 递增单位
    start_funds = re.findall(r"[^\n]+[递增][增加]", result)

    if start_funds:
        start = re.findall(r"\d+ [千百十]?万元", start_funds[0])
        increase = re.findall(r"(\d+ [千万百]?元)的整数倍", start_funds[0])
        if start:
            data["START_FUNDS_"] = start[0].replace(" ", "")
        if increase:
            data["INCREASE_"] = increase[0].replace(" ", "")

    # # 原始风险等级
    source_risk = re.findall(r"[低中高]{1,2}风险", result)
    if source_risk:
        # print(source_risk)
        data["SOURCE_RISK_LEVEL_"] = source_risk[0]
    # # 风险等级
    if data["SOURCE_RISK_LEVEL_"]:
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

    raise_date = re.findall(r"\d{4} ?年 \d{1,2} 月 \d{1,2} 日.*?[-—].*?日", result)
    if raise_date:
        r_date = re.findall(r"\d{4} 年 \d{1,2} 月 \d{1,2} 日", raise_date[0])
        # 募集起始日期
        r_start = re.sub(r"[日 ]", "", r_date[0])
        data["RAISE_START_"] = re.sub(r"[年月]", "-", r_start)
        # 募集结束日期
        r_end = re.sub(r"[日 ]", "", r_date[0])
        data["RAISE_END_"] = re.sub(r"[年月]", "-", r_end)

    all_date = re.findall(r"\d{4} 年 \d{1,2} 月 \d{1,2} 日", result)
    re_date = list()
    for each in all_date:
        each_rp = re.findall(r" \d ", each)
        for i in each_rp:
            each = each.replace(i, "0" + i.strip())
        each = re.sub(r"[年月日 ]", "", each)
        if each not in re_date:
            re_date.append(each)
    re_date.sort()

    if len(re_date) >= 3:
        # 产品起始日期
        data["PRO_START_"] = re_date[2]
        # 产品结束日期
        data["PRO_END_"] = re_date[-1]
    else:
        pass

    yield_ = re.findall(r"[1-9]\.\d{1,2}%", result)
    yield_.sort()
    if yield_:
        # 预期最低收益率
        data["YIELD_LOW_"] = yield_[0]
        # 预期最高收益率
        data["YIELD_HIGH_"] = yield_[-1]
    # 实际天数  本理财计划存续期为 7 年
    real_days = re.findall(r"(\d+ ?[年月日天]) ?\n", result)
    re_days = list()
    for days in real_days:
        if "年" in days:
            days = re.sub(r"[年 ]", "", days)
            days = int(days) * 365
            re_days.append(days)
        elif "月" in days:
            days = re.sub(r"[年 ]", "", days)
            days = int(days) * 365
            re_days.append(days)
        else:
            days = re.sub(r"[日天 ]", "", days)
            days = int(days)
            re_days.append(days)
    re_days.sort()
    if re_days:
        data["REAL_DAYS_"] = str(re_days[-1])

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZSYH_GW_ALL1", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
    # a = " 2019 年 1 月 31 日"
    # b = re.findall(r" \d ", a)
    # print(b)
    # for i in b:
    #     a = a.replace(i, "0"+i.strip())
    #     print(a)
