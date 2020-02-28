# -*- coding: utf-8 -*-
"""中国农业银行银行理财产品 JRCP_LCCP_ZGNYYH_GW_ALL"""
import re

from lxml.etree import HTML

from database._mongodb import MongoClient
from tools.req_for_api import req_for_something


def data_shuffle(data):
    if "PDF_" in data:
        # print(data["PDF_"])
        response = req_for_something(url=data["PDF_"])
        # print(response.text)
        try:
            pdf_content = response.content.decode("utf-8")
        except UnicodeDecodeError:
            pdf_content = response.content.decode("gbk")
        html = HTML(pdf_content)
        url = html.xpath("//a[contains(text(),\"说明书\")]/@href")
        if url:
            url = "http://ewealth.abchina.com/fs" + url[0][1:]
            # print(url)
            response2 = req_for_something(url=url)
            # print(response2.content.decode("utf-8"))
            try:
                response2_content = response2.content.decode("utf-8")
            except UnicodeDecodeError:
                response2_content = response2.content.decode("gbk")

            pdf_url1 = re.findall(r"/\w+\.pdf", response2_content)
            if pdf_url1:
                pdf_url = "http://ewealth.abchina.com/fs/intro_list" + pdf_url1[0]
                data["PDF_"] = pdf_url
        else:
            try:
                response_content = response.content.decode("utf-8")
            except UnicodeDecodeError:
                response_content = response.content.decode("gbk")
            url = re.findall(r"/\w+\.pdf", response_content)
            if url:
                pdf_url = "http://ewealth.abchina.com/fs/intro_list" + url[0]
                data["PDF_"] = pdf_url
        # response3 = req_for_something(url=pdf_url)
        # return response3
    if "RISK_LEVEL_" in data:
        if data["RISK_LEVEL_"] == "低":
            data["RISK_LEVEL_CODE_"] = "R1"
        elif data["RISK_LEVEL_"] == "中低":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["RISK_LEVEL_"] == "较低":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["RISK_LEVEL_"] == "中等":
            data["RISK_LEVEL_CODE_"] = "R3"
        elif data["RISK_LEVEL_"] == "中高":
            data["RISK_LEVEL_CODE_"] = "R4"
        elif data["RISK_LEVEL_"] == "高":
            data["RISK_LEVEL_CODE_"] = "R5"
    elif "SOURCE_RISK_LEVEL_" in data:
        if data["SOURCE_RISK_LEVEL_"] == "低":
            data["RISK_LEVEL_CODE_"] = "R1"
        elif data["SOURCE_RISK_LEVEL_"] == "中低":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["SOURCE_RISK_LEVEL_"] == "较低":
            data["RISK_LEVEL_CODE_"] = "R2"
        elif data["SOURCE_RISK_LEVEL_"] == "中等":
            data["RISK_LEVEL_CODE_"] = "R3"
        elif data["SOURCE_RISK_LEVEL_"] == "中高":
            data["RISK_LEVEL_CODE_"] = "R4"
        elif data["SOURCE_RISK_LEVEL_"] == "高":
            data["RISK_LEVEL_CODE_"] = "R5"
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_ZGNYYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
