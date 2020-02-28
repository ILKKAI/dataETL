# -*- coding: utf-8 -*-

# 贵州省招标投标公告服务平台网站 520000GZZBW

# 11320 条 有 URL_ & url 两个字段 url 打不开 CONTENT_ 字段里是 html 暂缓

import re
import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle(data):
    # 中标人清洗
    fina_result = ""
    result = re.findall(r"第?一?[中入]?[标围]?[候供][选应][人商单]位?名?称?[:：|]\|?(\w*-?\w*[(（]?\w*-?\w*[)）]?\w*-?\w*)", data["CONTENT_"])
    if result:
        if len(result) == 1:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or "集团" in result[0]:
                    fina_result = result[0]

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
                        "/" not in data["NOTICE_TIME_"]) and ("年" not in data["NOTICE_TIME_"]) and (
                        "CONTENT" not in data["NOTICE_TIME_"]) and ("公告" not in data["NOTICE_TIME_"]) and (
                        "作者" not in data["NOTICE_TIME_"]) and ("发布" not in data["NOTICE_TIME_"]) and (
                        "工程" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y%m%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("{" in data["NOTICE_TIME_"]) or ("[" in data["NOTICE_TIME_"]):
                    time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", data["NOTICE_TIME_"])
                    if time_result:
                        data["NOTICE_TIME_"] = time_result[0]
                else:
                    return None

            else:
                time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", str(data["NOTICE_TIME_"]))
                if time_result:
                    data["NOTICE_TIME_"] = time_result[0]
                else:
                    data["NOTICE_TIME_"] = ""
    if "NOTICE_TIME_" not in data:
        data["NOTICE_TIME_"] = ""

    # CONTENT_ 内容清洗
    data["CONTENT_"] = re.sub(r"</?[?a-zA-Z!].*?>|&nbsp;", "", str(data["CONTENT_"]))

    return data


def run():
    script = GenericScript(entity_code="520000GZZBW", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    data_list = data_shuffle(mongo_data_list)
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
