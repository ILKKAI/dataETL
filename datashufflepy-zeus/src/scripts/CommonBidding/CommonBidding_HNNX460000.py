# -*- coding: utf-8 -*-


#  海南省农村信用社联合社网站 HNNX460000
import re
import time

from scripts import GenericScript


def data_shuffle(data):
    if "WIN_CANDADITE_" not in data:
        data["WIN_CANDIDATE_"] = ""
    else:
        if data["WIN_CANDIDATE_"]:
            pass
        else:
            result = re.findall(r"[中入][标围]单位[:：]\|?(\w*[(（<《\-]?\w*[(（<《\-]?\w*)", data["CONTENT_"])
            if len(result) == 1:
                data["WIN_CANDIDATE_"] = result[0]
            elif len(result) > 1:
                pass
            if not result:
                result = re.findall(r"第一中标单位[:：为]\|?(\w*[(（<《\-]?\w*[(（<《\-]?\w*)", data["CONTENT_"])
                if len(result) == 1:
                    data["WIN_CANDIDATE_"] = result[0]
                elif len(result) > 1:
                    pass
            if not result:
                result = re.findall(r"入围供应商[:：]\|?(\w*[(（<《\-]?\w*[(（<《\-]?\w*)", data["CONTENT_"])
                if len(result) == 1:
                    data["WIN_CANDIDATE_"] = result[0]
                elif len(result) > 1:
                    pass
    data["WIN_CANDIDATE_"] = data["WIN_CANDIDATE_"].replace("|", "")
    data["WIN_CANDIDATE_"] = data["WIN_CANDIDATE_"].replace("。", "")

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
                        "工程" not in data["NOTICE_TIME_"]) and ("开始" not in data["NOTICE_TIME_"]) and (
                        "公示" not in data["NOTICE_TIME_"]):
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

    # 标题清洗
    # TODO IF ANYOTHER WORD RESULT TITLE IS NOT TRUE
    if (data["TITLE_"] == "中标公示") or (data["TITLE_"] == "中标公告") or (data["TITLE_"] == "流标公告"):
        re_title = ""
        result_title = re.findall(r"一、\|?(\w*[-\\、（(《]?\w*[-\\、)）》]?\w*[-\\、)）》]?\w*[-\\、)）》]?\w*[-\\、)）》]?\w*)", data["CONTENT_"])
        if result_title:
            if result_title[0] == "A标":
                result_title = []
            else:
                re_title = result_title[0]
        if not result_title:
            result_title = re.findall(r"结果如下：\|(\w*[-\\、（(《]?\w*[-\\、)）》]?\w*)", data["CONTENT_"])
            if result_title:
                if result_title[0] == "中标单位":
                    result_title = []
                else:
                    re_title = result_title[0]
        if not result_title:
            result_title = re.findall(r"项目名称：\|?(\w*[-\\、（(《]?\w*[-\\、)）》]?\w*)", data["CONTENT_"])
            if result_title:
                re_title = result_title[0]
        if not result_title:
            result_title = re.findall(r"《(\w*[-\\、（(《]?\w*[-\\、)）》]?\w*)》", data["CONTENT_"])
            if result_title:
                re_title = result_title[-1]
        data["TITLE_"] = data["TITLE_"] + "：" + re_title
        # print(data["TITLE_"])
        # print(data["CONTENT_"])
        # print(data["URL_"])
    elif data["TITLE_"] == "招标公告":
        re_title = ""
        result_title = re.findall(r"招标项目名称、性质、数量：\|?\s*[A-Z]?标?[:：]?(\w*[-\\、（(《]?\w*[-\\、)）》]?\w*)", data["CONTENT_"])
        if result_title:
            re_title = result_title[0]
        if not result_title:
            result_title = re.findall(r"招标项目名?称?、?内?容?：\|?\s*[A-Z]?标?[:：]?(\w*[-\\、（(《]?\w*[-\\、)）》]?\w*)", data["CONTENT_"])
            if result_title:
                re_title = result_title[0]
        data["TITLE_"] = data["TITLE_"] + "：" + re_title

    return data


def run():
    script = GenericScript(entity_code="HNNX460000", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    data_list = data_shuffle(mongo_data_list)


if __name__ == '__main__':
    run()
