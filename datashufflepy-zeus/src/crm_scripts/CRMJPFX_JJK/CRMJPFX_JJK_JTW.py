# -*- coding: utf-8 -*-
"""CRMJPFX_JJK_JTW 借记卡-金投网"""
from copy import deepcopy

from crm_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, ):
    '''
    处理_id, 爬取时间
    :param data:
    :return:
    '''
    re_data = deepcopy(data)
    re_data["SPIDER_TIME_"] = data["DATETIME_"]
    del re_data['DEALTIME_']
    del re_data['_id']
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    # CURRENCY_TYPE_
    #  卡等级
    if "CARD_LEVEL_" in re_data:
        if re_data["CARD_LEVEL_"] == "--":
            re_data["CARD_LEVEL_"] = ""
    else:
        re_data["CARD_LEVEL_"] = ""
    # 卡组织
    if re_data["CARD_BRAND_"] == "--":
        re_data["CARD_BRAND_"] = ""
    # 年费
    if re_data["FEE_"] == "--":
        re_data["FEE_"] = ""
    # 异地同行取现费
    if re_data["DISTANT_PEER_CASHING_"] == "--":
        re_data["DISTANT_PEER_CASHING_"] = ""
    # 异地跨行取现费
    if re_data["DISTANT_CROSS_CASHING_"] == "--":
        re_data["DISTANT_CROSS_CASHING_"] = ""
    # 异地同行转账费
    if re_data["DISTANT_PEER_TRANSFER_"] == "--":
        re_data["DISTANT_PEER_TRANSFER_"] = ""
    # 网银异地同行转账费
    if re_data["ONLINE_DISTANT_PEER_TRANSFER_"] == "--":
        re_data["ONLINE_DISTANT_PEER_TRANSFER_"] = ""
    # 挂失费
    if re_data["REPORT_LOSS_"] == "--":
        re_data["REPORT_LOSS_"] = ""
    # 账号管理费
    if re_data["ACCOUNT_MANAGE_"] == "--":
        re_data["ACCOUNT_MANAGE_"] = ""
    # 新卡工本费
    if re_data["CARD_FEE_"] == "--":
        re_data["CARD_FEE_"] = ""
    # 申请条件
    if re_data["APPLY_CONDITION_"] == "--" or re_data["APPLY_CONDITION_"].strip() == "":
        re_data["APPLY_CONDITION_"] = ""
    # 申请提交材料
    if re_data["APPLY_MATERIAL_"] == "--" or re_data["APPLY_MATERIAL_"].strip() == "":
        re_data["APPLY_MATERIAL_"] = ""
    # 单日取现限额（人民币账户）
    if re_data["MAX_CASHING_"] == "--" or re_data["MAX_CASHING_"].strip() == "":
        re_data["MAX_CASHING_"] = ""
    # 申请方式
    if re_data["APPLY_METHOD_"] == "--" or re_data["APPLY_METHOD_"].strip() == "":
        re_data["APPLY_METHOD_"] = ""
    # 销户方法
    if re_data["CANCEL_METHOD_"] == "--" or re_data["CANCEL_METHOD_"].strip() == "":
        re_data["CANCEL_METHOD_"] = ""
    # 挂失方法
    if re_data["REPORT_LOSS_METHOD_"] == "--" or re_data["REPORT_LOSS_METHOD_"].strip() == "":
        re_data["REPORT_LOSS_METHOD_"] = ""
    # 申请副卡最多数
    if re_data["MAX_APPLY_"] == "--" or re_data["MAX_APPLY_"].strip() == "":
        re_data["MAX_APPLY_"] = ""
    # 类型消费方式
    if re_data["PAY_METHOD_"] == "--" or re_data["PAY_METHOD_"].strip() == "":
        re_data["PAY_METHOD_"] = ""

    # 处理图片
    return re_data


if __name__ == '__main__':

    main_mongo = MongoClient(entity_code="CRMJPFX_JJK_JTW", mongo_collection="CRMJPFX_JJK")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data=data,)
        print(re_data)



