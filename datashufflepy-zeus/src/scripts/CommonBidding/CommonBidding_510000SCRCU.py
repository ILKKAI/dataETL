# -*- coding: utf-8 -*-

# 四川农村信用社网站 510000SCRCU  图片

#
import re
import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


def data_shuffle(data):
    fina_result = ""
    # 中标人清洗
    re_data = re.findall(r"中标人[:：](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])
    if not re_data:
        re_data = re.findall(r"致[:：](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])
    if not re_data:
        re_data = re.findall(r"成交供应商名称[:：]\s*([\w\s]*[(（《]?[\w\s]*[)）》]?[\w\s]*)", data["CONTENT_"], re.VERBOSE)
    if not re_data:
        re_data = re.findall(r"成交标的[:：](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])
    if not re_data:
        re_data = re.findall(r"[丰中]标人[：:|](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])
    if not re_data:
        re_data = re.findall(r"第一中标候选人[:：|](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])
    if not re_data:
        re_data = re.findall(r"第一名[:：](\w*[(（《]?\w*[)）》]?\w*)", data["CONTENT_"])
    if re_data:
        fina_result = re_data[0]

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
    script = GenericScript(entity_code="510000SCRCU", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    for data in mongo_data_list:
        data_list = data_shuffle(data)


if __name__ == '__main__':
    run()
