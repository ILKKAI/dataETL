# -*- coding: utf-8 -*-

# 基金产品  ICBCFUND

#from database._phoenix_hbase import PhoenixHbase
import hashlib
import pandas as pd
import re
from hbase.ttypes import Mutation, BatchMutation
from scripts import GenericScript


def data_shuffle(data, list_SUBS_STATUS, list_TYPE):
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
    if data["ESTABLISH_DATE_"]:
        re_data["PERIOD_CODE_"] = data["ESTABLISH_DATE_"][:10]
    else:
        re_data["PERIOD_CODE_"] = ""
    re_data["PERIOD_CODE_"] = re_data["PERIOD_CODE_"].replace("-", "")
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
    # 基金类型
    if data["TYPE_"]:
        re_data["TYPE_"] = data["TYPE_"]
    else:
        re_data["TYPE_"]=""
        re_data["TYPE_CODE_"] = ""
    # 基金类型编码
    for dict_TYPE in list_TYPE:
        if dict_TYPE["ITEM_LABEL_"] == data["TYPE_"]:
            re_data["TYPE_CODE_"] = dict_TYPE["ITEM_VALUE_"]
    # 最新净值
    if data["NEWEST_VALUE_"]:
         index = data["NEWEST_VALUE_"].find("（")
         if index != -1:
             re_data["NEWEST_VALUE_"] = data["NEWEST_VALUE_"][:index]
         else:
             re_data["NEWEST_VALUE_"] = data["NEWEST_VALUE_"]
    else:
        re_data["NEWEST_VALUE_"] = data["NEWEST_VALUE_"]
    # 累计净值
    re_data["TOTAL_VALUE_"] = data["TOTAL_VALUE_"]
    # 单位净值（日万份收益）
    re_data["UNIT_VALUE_"] = data["UNIT_VALUE_"]
    # 最新规模
    re_data["SCALE_"] = data["SCALE_"]
    # 成立日期
    re_data["ESTABLISH_DATE_"] = data["ESTABLISH_DATE_"]
    # 风险等级
    re_data["RISK_LEVEL_"] = data["RISK_LEVEL_"]
    # 日涨跌
    re_data["DAILY_RATE_"] = data["DAILY_RATE_"]
    #近三月
    re_data["QUARTER_RATE_"] = data["QUARTER_RATE_"]
    # 当前状态
    re_data["FUND_STATUS_"] = data["STATUS_"]
    # 基金公司
    re_data["COMPANY_"] = data["COMPANY_"]

    return re_data


def run():
    # entity_code 为 当前实体编码， entity_type 为当前实体所属类别， 对应 MongoDB 中集合名称
    script = GenericScript(entity_code="ICBCFUND", entity_type="JSFUND_CCBDATA")
    # 调用 GenericScript.data_from_mongo() 方法获取数据
    mongo_data_list = script.data_from_mongo()

    province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
    list_SUBS_STATUS = script.dict_from_mysql("FUND_SUBS_STATUS")
    list_TYPE = script.dict_from_mysql("FUND_TYPE")
    data_list = data_shuffle(mongo_data_list, list_SUBS_STATUS, list_TYPE)

    # pd.DataFrame(data_list).to_csv('E:\\LIANJIA_CLEAN_\\RONG360FINANCIAL.csv', encoding="utf_8_sig")
    # print("数据导入成功")

    # script.data_to_hbase(mongo_data_list)


if __name__ == '__main__':
    run()




