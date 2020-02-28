# -*- coding: utf-8 -*-

# 基金产品  STCNFUND

#from database._phoenix_hbase import PhoenixHbase
import hashlib
import pandas as pd
import re
from hbase.ttypes import Mutation, BatchMutation
from scripts import GenericScript


def data_shuffle(data,list_SUBS_STATUS,list_TYPE):
    dict_Bank = {'中国工商银行': 'ICBC', '工商银行': 'ICBC', '工行': 'ICBC', '中国农业银行': 'ICBC', '农业银行': 'ABC', '农行': 'ABC',
                 '中国银行': 'BOC', '中行': 'BOC', '中国建设银行': 'CCB',
                 '建设银行': 'CCB', '建行': 'CCB', '交通银行': 'BOCOM', '交行': 'BOCOM', '中国邮政储蓄银行': 'PSBC', '邮政储蓄银行': 'PSBC',
                 '邮政银行': 'PSBC', '邮蓄银行': 'PSBC', '浙商银行': 'CZB', '渤海银行': 'CBHB',
                 '中信银行': 'ECITIC', '中国光大银行': 'CEB', '光大银行': 'CEB', '华夏银行': 'HXB', '中国民生银行': 'CMBC', '民生银行': 'CMBC',
                 '招商银行': 'CMB', '招行': 'CMB', '兴业银行': 'CIB', '广发银行': 'CGB', '平安银行': 'PAB',
                 '上海浦东发展银行': 'SPDB', '浦东发展银行': 'SPDB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL'}

    re_data = dict()

    # ID
    hash_m = hashlib.md5()
    hash_m.update(data["ENTITY_NAME_"].encode("utf-8"))
    hash_title = hash_m.hexdigest()
    re_data["ID_"] = data["ENTITY_CODE_"] + "_" + str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(
        hash_title)
    # 公有列族 "C"
    # 实体编码
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    # 实体名字-后续添加的
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # 原网页地址
    re_data["URL_"] = data["URL_"]
    # 时间维度编码(发布时间)-只留了年月日
    # if data["SALE_START_"]:
    #     re_data["PERIOD_CODE_"] = data["SALE_START_"][:10]
    # else:
    #     re_data["PERIOD_CODE_"] = ""
    # re_data["PERIOD_CODE_"] = re_data["PERIOD_CODE_"].replace("-", "")
    # 数据处理状态， 统一为1
    re_data["STATUS_"] = "1"
    # 备注
    re_data["REMARK_"] = ""
    # 创建时间
    re_data["CREATE_TIME_"] = data["DATETIME_"]
    # 更新时间
    re_data["UPDATE_TIME_"] = ""

    # 基金代码
    re_data["CODE_"] = data["CODE_"]
    # 基金简称
    re_data["NAME_"] = data["NAME_"]
    # （T+1）日基金净值
    re_data["FUND_NEW_VALUE_"] = data["FUND_NEW_VALUE_"]
    # （T+1）日累计净值
    re_data["TOTAL_NEW_VALUE_"] = data["TOTAL_NEW_VALUE_"]
    # T日基金净值
    re_data["FUND_OLD_VALUE_"] = data["FUND_OLD_VALUE_"]
    # T日累计净值
    re_data["TOTAL_OLD_VALUE_"] = data["TOTAL_OLD_VALUE_"]
    # 日增长率（%）
    re_data["DAILY_RATE_"] = data["DAILY_RATE_"]
    #今年回报（%）
    re_data["YEAR_REWARD_"] = data["YEAR_REWARD_"]
    #申购状态
    re_data["SUBS_STATUS_"] = data["SUBS_STATUS_"]
    # 申购状态编码
    for dict_SUBS_STATUS in list_SUBS_STATUS:
        if dict_SUBS_STATUS["ITEM_LABEL_"] == data["SUBS_STATUS_"]:
            re_data["SUBS_STATUS_CODE_"] = dict_SUBS_STATUS["ITEM_VALUE_"]
        else:
            re_data["SUBS_STATUS_CODE_"] = ""
    #赎回状态
    re_data["ATONEM_STATUS_"] = data["ATONEM_STATUS_"]
    # 基金类型
    if data["TYPE_"]:
        re_data["TYPE_"] = data["TYPE_"]
    else:
        re_data["TYPE_"] = ""
        re_data["TYPE_CODE_"] = ""
    # 基金类型编码
    for dict_TYPE in list_TYPE:
        if dict_TYPE["ITEM_LABEL_"] == data["TYPE_"]:
            re_data["TYPE_CODE_"] = dict_TYPE["ITEM_VALUE_"]
        else:
            re_data["TYPE_CODE_"] = ""

    return re_data


def run():
    # entity_code 为 当前实体编码， entity_type 为当前实体所属类别， 对应 MongoDB 中集合名称
    script = GenericScript(entity_code="STCNFUND", entity_type="JSFUND_CCBDATA")
    # 调用 GenericScript.data_from_mongo() 方法获取数据
    mongo_data_list = script.data_from_mongo()

    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    list_SUBS_STATUS = script.dict_from_mysql("FUND_SUBS_STATUS")
    list_TYPE = script.dict_from_mysql("FUND_TYPE")
    data_list = data_shuffle(mongo_data_list,list_SUBS_STATUS,list_TYPE)

    # pd.DataFrame(data_list).to_csv('E:\\LIANJIA_CLEAN_\\RONG360FINANCIAL.csv', encoding="utf_8_sig")
    # print("数据导入成功")

    # script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()
