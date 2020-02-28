# -*- coding: utf-8 -*-

# 华融湘江银行网站 430100HRXJ  8 条

import re

import time

from scripts import GenericScript


def data_shuffle(data):
    fina_result = ""
    result = re.findall(r"第一中标候选人[:：]\|?(\w*[(（《]\w*[)）》]\w*)", data["CONTENT_"])
    if len(result) == 1:
        fina_result = result[0]

    if not result:
        result = re.findall(r"(\w*[(（《]\w*[)）》]\w*)\|中标\|", data["CONTENT_"])
        if len(result) == 1:
            fina_result = result[0]

    if not result:
        result = re.findall(r"\|中标候选人\|(.*)以上", data["CONTENT_"])
        if result:
            re_result = result[0].split("|")

            i = 0
            for r in re_result:
                if "公司" in r or "事务所" in r or "大学" in r or "车队" in r or "广播电台" in r or "电视台" in r or "报社广告中心" in r or "报社" in r or "代表处" in r:
                    fina_result = r
                    i += 1

                if i > 1:
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
    script = GenericScript(entity_code="430100HRXJ", entity_type="CommonBidding")

    mongo_data_list = script.data_from_mongo()
    data_shuffle(mongo_data_list)
    # script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()    
