# -*- coding: utf-8 -*-

# 重庆三峡银行网站 500000CQSX

# 3242 条 无 WIN_CANDIDATE_ 字段  2 条数据有有多个项目

# 时间 ['500000CQSX.CONTENT.NOTICE_TIME_', '2015-09-02']  待清洗

import re

import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle(data):
    fina_result = ""
    if "中标" in data["TITLE_"] or "结果" in data["TITLE_"]:

        result = re.findall("\|(.*)\|公示时间", data["CONTENT_"])

        re_result = result[0].split("|")
        count = 0
        for r in re_result:
            if ("公司" in r or "经营部" in r or "勘测院" in r or "事务所" in r or"大学" in r or "车队" in r
                    or "广播电台" in r or "电视台" in r or "报社" in r or "代表处" in r or "规划院" in r
                    or "出版社" in r or "研究院" in r or "中心" in r or "医院" in r or "园艺场" in r
                    or "开发局" in r or "设计院" in r or "饭店" in r or "门市" in r or "集团" in r
                    or "酒店" in r):
                count += 1
                if count == 1:
                    fina_result = r
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
    script = GenericScript(entity_code="500000CQSX", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        data_list = data_shuffle(data)

    # 创建 Phoenix 对象
    # p_client = PhoenixHbase(table_name="CommonBidding")
    # # 连接 Phoenix
    # connection = p_client.connect_to_phoenix()
    # # 插入数据
    # p_client.upsert_to_phoenix(connection=connection, data_list=data_list)
    # # 关闭连接
    # p_client.close_client_phoenix(connection=connection)


if __name__ == '__main__':
    run()    
