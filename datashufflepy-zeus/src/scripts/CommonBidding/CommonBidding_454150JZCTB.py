# -*- coding: utf-8 -*-

# 焦作中旅银行网站 454150JZCTB
# 19 条为 "" 可父级
# 51 条数据 有 WIN_CANDIDATE_ 但都为空

import re

import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle(data):
    fina_result = ""
    result = re.findall(r"第一入围供应商[:：](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"中标单位\w*[:：|]\w*[:：|](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"入围单位\w*[:：|]\w*[:：|](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])

    if not result:
        result = re.findall(r"第一中标候选人[:：|](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])

    if result:
        if len(result) == 1:
            if "标段" in result[0]:
                fina_result = ""
            elif "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or "报社广告中心" in result[0] or "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or "研究院" in result[0] or "整治中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or "开发局" in result[0] or "设计院" in result[0]:
                fina_result = result[0]

            else:
                fina_result = ""
        else:
            fina_result = ""

    if not result:
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
                        "/" not in data["NOTICE_TIME_"]) and ("年" not in data["NOTICE_TIME_"]):
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
    script = GenericScript(entity_code="454150JZCTB", entity_type="CommonBidding")

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
