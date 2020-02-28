# -*- coding: utf-8 -*-

# 山东省农村信用社联合社网站 370000SDNXS

# 122 条为"" 1 条无 NOTICE_TIME_ 字段 待清洗

import time
import re
from scripts import GenericScript


def data_shuffle(data):
    pattern1 = re.compile(r'中标结果：\|(.*?公司)')
    pattern2 = re.compile(r'中标结果：\|中标人：(.*?公司)')

    WIN_candidate = ""
    if "中标结果" in data["CONTENT_"]:
        # print(data["CONTENT_"])
        handle_list = pattern1.findall(data["CONTENT_"], re.S)
        if handle_list:
            WIN_candidate = handle_list[0]
        # if handle_list == []:
        handle_list = pattern2.findall(data["CONTENT_"], re.S)
        if handle_list:
            WIN_candidate = handle_list[0]

        if len(WIN_candidate) > 50:
            WIN_candidate = ""
        if "1.1" in WIN_candidate:
            WIN_candidate = ""

        if "（" in WIN_candidate:
            WIN_candidate = WIN_candidate.split("（")[0]

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
    script = GenericScript(entity_code="370000SDNXS", entity_type="CommonBidding")

    mongo_data_list = script.data_from_mongo()
    data_shuffle(mongo_data_list)
    # script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()    
