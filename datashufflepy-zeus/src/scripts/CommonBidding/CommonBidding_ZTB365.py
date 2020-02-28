# -*- coding: utf-8 -*-

#  中国电子招标信息网 ZTB365

#  无 WIN_CANDIDATE_ 字段 原网站信息被隐藏


import re
import time

from scripts import GenericScript


def data_shuffle(data):
    if "WIN_CANDIDATE_" not in data:
        data["WIN_CANDIDATE_"] = ""
    result = re.findall(r"中 ?标 ?人[:：](\w*[(（《<\-]?\w*[)）》>\-]?\w*)", data["CONTENT_"])
    if len(result) == 1:
        data["WIN_CANDIDATE_"] = result[0]
        win_check = data["WIN_CANDIDATE_"].split("公司")
        if len(win_check) > 2:
            data["WIN_CANDIDATE_"] = ""
    elif len(result) > 1:
        data["WIN_CANDIDATE_"] = ""
    if not result:
        result = re.findall(r"中标候选人[:：](\w*[(（《<\-]?\w*[)）》>\-]?\w*)", data["CONTENT_"])
        if len(result) == 1:
            data["WIN_CANDIDATE_"] = result[0]
        elif len(result) > 1:
            pass
    if not result:
        result = re.findall(r"中标单位如下[:：](\w*[(（<《\-]?\w*[(（<《\-]?\w*)", data["CONTENT_"])
        if len(result) == 1:
            data["WIN_CANDIDATE_"] = result[0]
        elif len(result) > 1:
            pass
    if not result:
        result = re.findall(r"中标情况[:：].*?\*(\w*[(（<《\-]?\w*[(（<《\-]?\w*)", data["CONTENT_"])
        if len(result) == 1:
            data["WIN_CANDIDATE_"] = result[0]
        elif len(result) > 1:
            pass

    if data["WIN_CANDIDATE_"]:
        win_index = data["WIN_CANDIDATE_"].find("中标")
        if win_index != -1:
            data["WIN_CANDIDATE_"] = data["WIN_CANDIDATE_"][:win_index]

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
                        "工程" not in data["NOTICE_TIME_"]) and ("开始" not in data["NOTICE_TIME_"]):
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

    return data


def run():
    script = GenericScript(entity_code="ZTB365", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    data_list = data_shuffle(mongo_data_list)


if __name__ == '__main__':
    run()
