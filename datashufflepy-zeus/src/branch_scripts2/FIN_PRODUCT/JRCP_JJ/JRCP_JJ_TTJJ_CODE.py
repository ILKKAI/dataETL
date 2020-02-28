# -*- coding: utf-8 -*-
"""天天基金历史净值数据总"""
import re
import json
import time

from database._mongodb import MongoClient


def fomat_time(time_stamp):
    """

    :param time_stamp: 时间戳
    :return: 格式化的时间
    """
    if len(str(time_stamp)) == 13:
        time_stamp = time_stamp // 1000

    return time.strftime('%Y-%m-%d', time.localtime(time_stamp))


def data_shuffle(data):
    data["PRO_NAME_"] = data["name"]
    data["PRO_CODE_"] = data["code"]
    all_data = data.pop("all_data")
    # Data_netWorthTrend 净值
    # Data_ACWorthTrend 累计净值
    # Data_millionCopiesIncome 每万份收益
    # Data_sevenDaysYearIncome 7日年化收益率
    search_list = ["Data_netWorthTrend", "Data_ACWorthTrend", "Data_millionCopiesIncome", "Data_sevenDaysYearIncome"]
    for field in search_list:
        re_list = re.findall(r"(?<={} = ).*?(?=;)".format(field), all_data.replace("\n", ""))
        if re_list and isinstance(re_list, list):
            data[field] = json.loads(re_list[0].replace(" ", ""))
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_JJ_TTJJ_CODE", mongo_collection="JRCP_JJ_ALL_DATA")
    data_list = main_mongo.main()
    data = data_list[0]
    re_data = data_shuffle(data)
    for item in re_data["Data_netWorthTrend"]:
        print(fomat_time(item["x"]))

    # print(re_data)
