# -*- coding: utf-8 -*-

# 上海农商银行网站 310000SRCB

# 1条 ['310000SRCB.CONTENT.NOTICE_TIME_', '2018-05-17'] 可父级

import re
import time

from scripts import GenericScript


def data_shuffle(data):
    pattern = re.compile(r"经招标人最终确定：(.*?)为本项目中标")

    WIN_candidate = ""
    if "结果" in data["TITLE_"] or "中标" in data["TITLE_"]:
        # print(data["CONTENT_"])
        shuffle_str = data["CONTENT_"].split("|")
        for shuff in shuffle_str:
            if "最终" in shuff:
                WINNER = pattern.findall(shuff)
                if WINNER:
                    WIN_candidate = WINNER[0]

                    # WIN_candidate = list(set(WIN_candidate))  #去重
    data["WIN_CANDIDATE_"] = WIN_candidate
    #print(data["WIN_CANDIDATE_"])

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
    script = GenericScript(entity_code="310000SRCB", entity_type="CommonBidding")

    mongo_data_list = script.data_from_mongo()
    data_shuffle(mongo_data_list)
    # script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
