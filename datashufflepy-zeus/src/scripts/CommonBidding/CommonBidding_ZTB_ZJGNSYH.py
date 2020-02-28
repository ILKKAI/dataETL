# -*- coding: utf-8 -*-

#  张家港农商银行-重要公告 ZTB_ZJGNSYH

#  无 WIN_CANDIDATE_ 字段 原网站信息被隐藏
# 张家港农商银行-重要公告

import re
import time

from database._mongodb import MongoClient
from scripts import GenericScript

"""
江苏张家港农村商业银行股份有限公司资金头寸管理系统项目选型公告|根据|江苏张家港农村商业银行股份有限公司|业务发展的需求，现就|我行|“|资金头寸管理系统|”|进行选型公告：|1.|公告编号：|ZJGRCB20190730|2.|公告人：|江苏张家港农村商业银行股份有限公司|3.|项目实施地点：江苏省张家港市人民中路|66|号|4.|公告开始时间：|2019|年|7|月|3|1|日|5.|公告截止时间：|2019|年|8|月|12|日|6.|公告人联系方式：|江苏张家港农村商业银行股份有限公司|地址：|江苏省张家港市人民中路|66|号|邮政编码：|215600|联系人：陈晓钢|电话号码：|0512-56968017|邮箱：|574181487@qq.com|技术联系人：周元君|电话号码：|0512-35008103|手机：|18963693751|邮箱：|yuanjun.zhou@foxmail.com|7.|招标文件下载|"""


def data_shuffle(data):
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

    data["TITLE_"] = data["TITLE_"].replace("|", "")
    if "CONTENT_" in data:
        data["CONTENT_"] = data["CONTENT_"].replace("|", "")

    return data


def run():
    main_mongo = MongoClient(entity_code="ZTB_ZJGNSYH", mongo_collection="CommonBidding")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)


if __name__ == '__main__':
    run()
