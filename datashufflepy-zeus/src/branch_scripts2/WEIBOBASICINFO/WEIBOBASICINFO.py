# -*- coding: utf-8 -*-
"""微博基本信息 WEIBOBASICINFO"""
import re
import sys
import os
import time


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-15])

from database._mongodb import MongoClient
from log.data_log import Logger
from branch_scripts2 import GenericScript
from tools.req_for_api import req_for_serial_number


class WeiboBasicInfoScript(GenericScript):
    # 初始化参数
    def __init__(self, table_name, collection_name, param):
        self.logger = Logger().logger
        self.remove_id_list = list()
        self.copy_mongo_data_list = list()
        self.branch_code_list = list()
        self.find_count = 0
        self.bad_count = 0
        self.success_count = 0
        self.remove_count = 0
        self.old_count = 0
        self.name_dict = {'工行': 'ICBC', '工商银行': 'ICBC', '农行': 'ABC', '农业银行': 'ABC', '中行': 'BOC', '中银': 'BOC',
                          '建行': 'CCB', '邮政储蓄银行': 'PSBC',
                          '建信': 'CCB', '建设银行': 'CCB', '交行': 'BCM', '交通银行': 'BCM', '邮储银行': 'PSBC', '浙商银行': 'CZB',
                          '渤海银行': 'CBHB', '中信银行': 'ECITIC', '光大银行': 'CEB', '华夏银行': 'HB', '招行': 'CMB', '招商银行': 'CMB',
                          '兴业银行': 'CIB', '广发银行': 'CGB', '平安银行': 'PAB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL',
                          '浦东发展银行': 'SPDB', '民生银行': 'CMBC', '汇丰银行': 'HSBC', '渣打银行': 'SC', '南海农商银行':'NRC ',
                          '顺德农村商业银行': 'sdebank',
                          }
        super(WeiboBasicInfoScript, self).__init__(table_name=table_name, collection_name=collection_name, param=param, verify_field={"WEIBO_CODE_": "WEIBO_CODE_"})

    def match_weibo_code(self, match):
        mongo_client = MongoClient(mongo_collection="WEIBOBASICINFO")
        db, collection_list = mongo_client.client_to_mongodb()
        collection = mongo_client.get_check_collection(db, collection_list)
        result = mongo_client.match_from_mongo(collection=collection, match=match, output="WEIBO_CODE_")
        return result

    def generic_shuffle(self, data):
        re_data = dict()
        # print(data)
        # print(data["DEALTIME_"])
        time_array = time.localtime(int(data["DEALTIME_"]))
        period_time = time.strftime("%Y%m%d", time_array)

        serial_number = req_for_serial_number(code="WEIBO_BASIC_INFO")
        re_data["ID_"] = serial_number

        # 对BANK_NAME_作处理
        # 对特殊微信BANK_NAME 做处理
        for key, value in self.name_dict.items():
            if key[:2] in data["ENTITY_NAME_"]:
                re_data["BANK_NAME_"] = key
                re_data["BANK_CODE_"] = value
                break
        if "BANK_NAME_" in re_data:
            if re_data["BANK_NAME_"] == "建信":
                re_data["BANK_NAME_"] = "中国建设银行"
            if re_data["BANK_NAME_"] == "建行":
                re_data["BANK_NAME_"] = "中国建设银行"
            if re_data["BANK_NAME_"] == "建设银行":
                re_data["BANK_NAME_"] = "中国建设银行"
            if re_data["BANK_NAME_"] == "农行":
                re_data["BANK_NAME_"] = "中国农业银行"
            if re_data["BANK_NAME_"] == "农业银行":
                re_data["BANK_NAME_"] = "中国农业银行"
            if re_data["BANK_NAME_"] == "工行":
                re_data["BANK_NAME_"] = "中国工商银行"
            if re_data["BANK_NAME_"] == "工商银行":
                re_data["BANK_NAME_"] = "中国工商银行"
            if re_data["BANK_NAME_"] == "民生银行":
                re_data["BANK_NAME_"] = "中国民生银行"
            if re_data["BANK_NAME_"] == "光大银行":
                re_data["BANK_NAME_"] = "中国光大银行"
            if re_data["BANK_NAME_"] == "交行":
                re_data["BANK_NAME_"] = "交通银行"
            if re_data["BANK_NAME_"] == "招行":
                re_data["BANK_NAME_"] = "招商银行"
            if re_data["BANK_NAME_"] == "农行":
                re_data["BANK_NAME_"] = "中国农业银行"
            if re_data["BANK_NAME_"] == "中行":
                re_data["BANK_NAME_"] = "中国银行"
            if re_data["BANK_NAME_"] == "中银":
                re_data["BANK_NAME_"] = "中国银行"
            if re_data["BANK_NAME_"] == "邮储银行":
                re_data["BANK_NAME_"] = "中国邮政储蓄银行"
            if re_data["BANK_NAME_"] == "邮政储蓄银行":
                re_data["BANK_NAME_"] = "中国邮政储蓄银行"
            if re_data["BANK_NAME_"] == "南海农商银行":
                re_data["BANK_NAME_"] = "广东南海农村商业银行股份有限公司"
            if re_data["BANK_NAME_"] == "顺德农村商业银行":
                re_data["BANK_NAME_"] = "广东顺德农村商业银行股份有限公司"


        re_data["PERIOD_CODE_"] = period_time
        # 数据来源 URL
        source = re.findall(r"(https?://.*?)/", data["MAIN_URL_"])
        re_data["SOURCE_"] = source[0]
        # 数据来源 网站名称
        re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"].split("-")[0]

        re_data["SOURCE_TYPE_"] = ""
        re_data["HOT_"] = "0"
        re_data["WEIBO_CODE_"] = data["WEIBO_CODE_"]
        re_data["WEIBO_NAME_"] = data["ENTITY_NAME_"]
        re_data["FOCUS_"] = data["FOCUS_"]
        re_data["FANS_"] = data["FANS_"]
        # 对错误COMPANY 处理
        if re.match(r"\d+-\d+-\d+", data["COMPANY_"]):
            data["COMPANY_"] = data["ENTITY_NAME_"] + "股份有限公司"
        re_data["COMPANY_"] = data["COMPANY_"]
        re_data["VIRIFIED_"] = data["VIRIFIED_"]
        re_data["BRIEF_"] = data["BIREF_"]
        re_data["VERSION_"] = "0"
        # 添加大V认证 默认银行官微都为大V
        re_data["VERIFIED_"] = "Y"
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        if re_data["ENTITY_NAME_"] == "华夏银行":
            re_data["ENTITY_NAME_"] = "华夏银行微博"
        re_data["URL_"] = data["MAIN_URL_"]
        re_data = super(WeiboBasicInfoScript, self).generic_shuffle(data=data, re_data=re_data, field="ENTITY_NAME_")
        return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]


if __name__ == '__main__':
    param = sys.argv[1]
    # param = "{'limitNumber':'1'}"
    script = WeiboBasicInfoScript(table_name="CHA_BRANCH_WEIBO_BASIC", collection_name="WEIBOBASICINFO", param=param)
    script.main()
