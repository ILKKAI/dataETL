# -*- coding: utf-8 -*-

# 华夏银行网站 110000HXYH
# 3 条 为 ['110000HXYH.CONTENT.NOTICE_TIME_', '2018-10-30'] 父级可获取
# WIN_CANDIDATE_ 不准确 已完成 65 条数据

import re

import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle(data):
    fina_result = ""
    if "结果" in data["TITLE_"] or "中标" in data["TITLE_"]:
        if "01包" in data["CONTENT_"] or "第一包" in data["CONTENT_"] or "04包" in data["CONTENT_"] or "第1包" in data["CONTENT_"]:
            fina_result = ""

        elif "候选" in data["CONTENT_"] and "第一名" in data["CONTENT_"] or "第一中标候选人" in data["CONTENT_"] or "第一成交供应商" in data["CONTENT_"]:
            result = re.findall(r"第一\w*[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])
            fina_result = result[0]

        else:
            result = re.findall(r"成交[采]?[购]?供应商[为]?[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])

            if not result:
                result = re.findall(r"中[选标]供应商[为]?[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])

            if not result:
                result = re.findall(r"成交[单结][位果][为]?[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])

            if not result:
                result = re.findall(r"入围供应商[为]?[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])

            if not result:
                result = re.findall(r"供应商为[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])

            if not result:
                result = re.findall(r"供应商[:：]?[|]?(\w*[（(]?\w*[)）]?\w*)", data["CONTENT_"])

            if result:
                fina_result = result[0]
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
                        "/" not in data["NOTICE_TIME_"]):
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
    script = GenericScript(entity_code="110000HXYH", entity_type="CommonBidding")

    # # 从 MongoDB 获取数据
    # mongo_data_list = script.data_from_mongo()
    # data_list = data_shuffle(mongo_data_list)
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
