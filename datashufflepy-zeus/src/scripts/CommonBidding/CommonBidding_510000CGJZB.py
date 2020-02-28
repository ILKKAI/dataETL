# -*- coding: utf-8 -*-

# 四川国际招标有限责任公司网站 510000CGJZB 11984 条 

# WIN_CANDIDATE_ 字段为小驼峰式

import re

import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle(data):
    fina_result = ""
    result = re.findall(r"中标人名?称?[:：](\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"[中成]?[标交]?供应商名?称?[:：](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"第一[中成]?[标交]?候选人[:：|](\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"成交人名?称?[:：|](\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"成交候选人[:：|](\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"第一名[:：|](\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])

    if result:
        if len(result) == 1:
            re_result = re.sub(r"[中投成][标交].*|地址.*|投标报价.*|建安费.*|[0-9]?[0-9]包", "", result[0])
            fina_result = re_result
        else:
            fina_result = ""

    data["WIN_CANDIDATE_"] = fina_result

    # 发布时间清洗
    if "NOTICE_TIME_" in data:
        if data["NOTICE_TIME_"]:
            if isinstance(data["NOTICE_TIME_"], str):
                if ("-" in data["NOTICE_TIME_"]) and ("{" not in data["NOTICE_TIME_"]) and (
                        "[" not in data["NOTICE_TIME_"]):
                    pass
                elif "年" in data["NOTICE_TIME_"] and ("至" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y年%m月%d日")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif "/" in data["NOTICE_TIME_"]:
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y/%m/%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif "\\" in data["NOTICE_TIME_"]:
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y\%m\%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("-" not in data["NOTICE_TIME_"]) and ("\\" not in data["NOTICE_TIME_"]) and (
                        "/" not in data["NOTICE_TIME_"]) and ("年" not in data["NOTICE_TIME_"]) and ("CONTENT" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y%m%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("{" in data["NOTICE_TIME_"]) or ("[" in data["NOTICE_TIME_"]):
                    time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", data["NOTICE_TIME_"])
                    if time_result:
                        data["NOTICE_TIME_"] = time_result[0]

            else:
                time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", str(data["NOTICE_TIME_"]))
                if time_result:
                    data["NOTICE_TIME_"] = time_result[0]
                else:
                    data["NOTICE_TIME_"] = ""
    if "NOTICE_TIME_" not in data:
        data["NOTICE_TIME_"] = ""

    return data


def run():
    script = GenericScript(entity_code="510000CGJZB", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    data_list = data_shuffle(mongo_data_list)
    # # 创建 Phoenix 对象
    # p_client = PhoenixHbase(table_name="CommonBidding")
    # # 连接 Phoenix
    # connection = p_client.connect_to_phoenix()
    # # 插入数据
    # p_client.upsert_to_phoenix(connection=connection, data_list=data_list)
    # # 关闭连接
    # p_client.close_client_phoenix(connection=connection)


if __name__ == '__main__':
    run()    
