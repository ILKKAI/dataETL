# -*- coding: utf-8 -*-

# 中国银行理财产品 BOCFinancial
# 有 CONTENT_ 需清洗  已完成  1876 条 PDF

import hashlib
import re
from time import sleep

from datetime import timedelta

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle_no_pdf(data):
    if not data["CONTENT_"]:
        # continue
        return None
    else:
        # print("*"*30)
        # print(data["CONTENT_"])
        # print(data["URL_"])

        if data["CONTENT_"][:2] == "产品":
            first_shuffle = data["CONTENT_"].split()
            # 多个识别码，多个投资结束日，多个收益率
            # print(first_shuffle)

            product_list = re.findall(r"\|([A-Z]+[0-9]*-?[A-Z0-9]*-?[A-Z0-9]*)", data["CONTENT_"])
            re_product_list = list()
            for product in product_list:
                if (not product.isalpha()) and (not product.isnumeric()):
                    re_product_list.append(product)

            yield_rate_list = re.findall(r"\|([0-9]\.[0-9][0-9]*%[或\-]?[1-9]?\.?\d*%?)|\|([\\/])", data["CONTENT_"])
            for y in yield_rate_list:
                i, j = y
                if i and ("%" in i) and ("或" not in i):
                    i = i.replace("%", "")
                    i = float(i)
                    if i < 0.5:
                        yield_rate_list.remove(y)
                elif j and ("%" in j) and ("或" not in i):
                    j = j.replace("%", "")
                    j = float(j)
                    if j < 0.5:
                        yield_rate_list.remove(y)

            end_date_list = re.findall(r"\|(\d{4}[-/年][0-2]?[0-9][-/月][0-3]?[0-9][日]?至?"
                                       r"\d{0,4}[-/年]?[0-2]?[0-9]?[-/月]?[0-3]?[0-9]?[日]?)", data["CONTENT_"])

            day_list = re.findall(r"\|(\d*[天月年])\|", data["CONTENT_"])

            # print(re_product_list)
            # print(yield_rate_list)
            # print(end_date_list)
            # print(day_list)
            # print(len(re_product_list), len(yield_rate_list), len(end_date_list), len(day_list))
            # print(data["URL_"])

            p_dict = dict()
            p_list = list()
            for product in product_list:
                p_dict["INDEX_"] = product_list.index(product)
                p_dict["CODE_"] = product
                try:
                    p_dict["YIELD_RATE_"] = yield_rate_list[p_dict["INDEX_"]]
                except IndexError:
                    p_dict["YIELD_RATE_"] = ""
                try:
                    p_dict["DAY_"] = day_list[p_dict["INDEX_"]]
                except IndexError:
                    p_dict["DAY_"] = ""
                if (len(yield_rate_list) / len(product_list) == 2):
                    for product in product_list:
                        yield_rate_list.pop(int(product_list.index(product)) + 1)
                for rate in yield_rate_list:
                    i, j = rate
                    if i:
                        p_dict["YIELD_RATE_"] = i
                    elif j:
                        p_dict["YIELD_RATE_"] = j
                multiple = len(end_date_list) / len(product_list)
                if multiple == 1:
                    try:
                        p_dict["SALE_START_"] = end_date_list[p_dict["INDEX_"]]
                        p_dict["SALE_END_"] = ""
                        p_dict["PRO_START_"] = ""
                        p_dict["PRO_END_"] = ""
                    except IndexError:
                        p_dict["SALE_START_"] = end_date_list[p_dict["INDEX_"]]
                        p_dict["SALE_END_"] = ""
                        p_dict["PRO_START_"] = ""
                        p_dict["PRO_END_"] = ""
                elif multiple == 2:
                    try:
                        p_dict["SALE_START_"] = end_date_list[int(p_dict["INDEX_"] * multiple)]
                        p_dict["SALE_END_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 1]
                        p_dict["PRO_START_"] = ""
                        p_dict["PRO_END_"] = ""
                    except IndexError:
                        p_dict["SALE_START_"] = end_date_list[int(p_dict["INDEX_"] * multiple)]
                        p_dict["SALE_END_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 1]
                        p_dict["PRO_START_"] = ""
                        p_dict["PRO_END_"] = ""
                elif multiple == 3:
                    p_dict["SALE_START_"] = end_date_list[int(p_dict["INDEX_"] * multiple)]
                    p_dict["SALE_END_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 1]
                    p_dict["PRO_START_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 2]
                    p_dict["PRO_END_"] = ""
                elif multiple == 4:
                    p_dict["SALE_START_"] = end_date_list[int(p_dict["INDEX_"] * multiple)]
                    p_dict["SALE_END_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 1]
                    p_dict["PRO_START_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 2]
                    p_dict["PRO_END_"] = end_date_list[int(p_dict["INDEX_"] * multiple) + 3]
                else:
                    print(re_product_list)
                    print(yield_rate_list)
                    print(end_date_list)
                    print(day_list)
                    print(len(re_product_list), len(yield_rate_list), len(end_date_list), len(day_list))
                    print(data["URL_"])

                p_list.append(p_dict)
            return p_list

            # with open("C:/Users/kevin/Desktop/financial/中国银行_CBHB.txt", "a", encoding="utf-8") as f:
            #     f.write(str(p_dict))
            #     f.write("\n\n")


def data_shuffle(data):
    product_code = ""
    product_name = ""
    time_limit = ""
    yield_rate = ""
    start_funds = ""
    inverst_period = ""
    risk_level = ""

    re_data = dict()
    if ("PDF_BASE64_" not in data) or (not data["CONTENT_"]):
        return None
    else:
        # 产品代码
        product_code = re.search(r"\n(【?[A-Za-z0-9-]{5,}】?) *\n", data["CONTENT_"])
        if not product_code:
            product_code = re.search(r"【产品代码：(.*?)】", data["CONTENT_"])
        # 登记编码  【C1010418006452】
        registration_code = re.search(r"C\d{13}", data["CONTENT_"])

        # 产品名称
        product_name = re.search(r"\n([^\n]*)\n【?[A-Za-z0-9-]{5,}】? *\n", data["CONTENT_"])
        if product_name:
            #  登记系统登记编码  理财产品代码  产品代码   1、产品基本信息
            if ("码" in product_name.group()) or ("基本信息" in product_name.group()):
                product_name = re.search(r"([^\n]*)\n?产\n?品\n?说\n?明\n?书", data["CONTENT_"])
                # print("NAME_", product_name.group(1))
        else:
            product_name = re.search(r"([^\n]*)\n?产\n?品\n?说\n?明\n?书", data["CONTENT_"])

        # 匹配表格中内容
        # 一、理财计划基本信息 二、理财计划投资范围、投资种类及比例  [投理][资财][对计][象划]及?投资[限范][制围]
        first_shuffle = re.findall(r"[一1]、 *理?财?[产计][品划]基本信息(.*)\n[二2]、", data["CONTENT_"], re.S)
        re_time = None
        if first_shuffle:
            # 风险等级
            # 保证收益型   非保本浮动收益型  无固定期限非保本浮动收益型，实际产品期限受制于提前终止条款。
            # break_even = re.findall(r"产品类型 *\n([^\n]*)", first_shuffle[0])
            break_even = re.findall(r"\n(\w*收益型)[；，]?\w*。? *\n", first_shuffle[0])

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
                re_time = re.findall(r"\d{4} ?年\d{1,2} ?月?\d{1,2} ? 日?", time_limit[0])
                if not re_time:
                    re_time = re.findall(r"(【?\d{4}】? ?[年\\/-] ?【?\d{1,2}】? ?[月\\/-] ?【?\d{1,2}】? ?日?)", time_limit[0])
            # else:
            #     re_time = re.findall(r"(【?\d{4}】? ?[年\\/-] ?【?\d{1,2}】? ?[月\\/-] ?【?\d{1,2}】? ?日?)", first_shuffle[0])

            # 期限
            inverst_period = re.findall(r"\n(【?\d*】? ?天（?.*）?) *\n", first_shuffle[0])
            # 收益率
            yield_rate = re.findall(r"(【?\d\.\d{2}】?%) *\n", first_shuffle[0])
            # 售卖区域
            # re_data["SALE_DISTRICT_"] = data["SALE_AREA_"]
            # 是否保本
            # re_data["BREAKEVEN_"] = data["BREAKEVEN_"]
            # 起购金额
            start_funds = re.findall(r"(起点[^\n]*倍[递累][^\n]*)", first_shuffle[0])
            # 可否赎回
            # re_data["REDEMING_MODE_"]
            # 私人银行
            # re_data["PRIVATE_BANK_"]
        else:
            with open("verify_id.txt", "a", encoding="utf-8", errors="ignore")as f:
                f.write(data["CONTENT_"])
            return None
        # "C"
        if product_name:
            product_name = re.sub("[【】 ]", "", product_name.group(1))
        if product_code:
            product_code = re.sub("[【】 ]", "", product_code.group(1))

        # hash_m = hashlib.md5()
        # hash_m.update(product_name.encode("utf-8"))
        # hash_id = hash_m.hexdigest()
        if re_time:
            # 开始售卖时间
            start_time = re.sub(r"[【】 ]", "", re_time[0])
            start_time = re.sub(r"[年月日]", "-", start_time)
            re_data["ID_"] = "BOC" + "_" + product_code + "_" + start_time
            re_data["PERIOD_CODE_"] = start_time.replace("-", "")
        else:
            re_data["ID_"] = "BOC" + "_" + product_code
            re_data["PERIOD_CODE_"] = ""
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        # re_data["AREA_CODE_"]
        re_data["BANK_CODE_"] = "BOC"
        re_data["BANK_NAME_"] = "中国银行"
        # re_data["UNIT_CODE_"]
        # re_data["CONTENT_"]
        re_data["STATUS_"] = ""
        # re_data["REMARK_"]
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        # re_data["UPDATE_TIME_"]

        # "F"
        re_data["CODE_"] = product_code[0]
        re_data["NAME_"] = product_name[0]
        # 售卖时间范围
        if time_limit:
            re_data["TIME_LIMIT_"] = time_limit[0]
        if re_time:
            # 开始售卖时间
            re_data["SALE_START_"] = re.sub(r"[【】 ]", "", re_time[0])
            if len(re_time) > 1:
                # 结束售卖时间
                re_data["SALE_END_"] = re.sub(r"[【】 ]", "", re_time[1])
                # 起息日 re_time[2]
                # 结息日 re_time[3]
        # 收益率
        if yield_rate:
            if len(yield_rate) == 1:
                re_data["YIELD_RATE_"] = re.sub(r"[【】 ]", "", yield_rate[0])
            else:
                re_data["YIELD_RATE_"] = re.sub(r"[【】 ]", "", yield_rate[0]) + "-" + re.sub(r"[【】 ]", "", yield_rate[1])
        # 售卖区域
        # re_data["SALE_AREA_"] = data["SALE_AREA_"]
        # 是否保本
        re_data["BREAKEVEN_"] = break_even[0].replace(" ", "")
        # 起购金额
        if start_funds:
            re_data["START_FUNDS_"] = start_funds[0]
        # 期限
        if inverst_period:
            re_data["INVEST_PERIOD_"] = inverst_period[0]
        # 风险等级
        #     re_data["RISK_LEVEL_"] = ""
        re_data["URL_"] = data["URL_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["DATETIME_"] = data["DATETIME_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]

        # 无
        # 可否赎回
        # re_data["REDEMING_MODE_"]
        # 私人银行
        # re_data["PRIVATE_BANK_"]
        # print(re_data)

        return re_data


def run(p_client, connection):
    script = GenericScript(entity_code="BOCFinancial", entity_type="FINPRODUCT_FINASSIST")
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        re_data = data_shuffle(data)
        if re_data:
            p_client.upsert_to_phoenix_by_one(connection=connection, data=re_data)


if __name__ == '__main__':
    # 创建 Phoenix 对象
    p_client = PhoenixHbase(table_name="TEST_FINPRODUCT_FINASSIST")
    # 连接 Phoenix
    connection = p_client.connect_to_phoenix()
    run(connection=connection, p_client=p_client)
    connection.close()
