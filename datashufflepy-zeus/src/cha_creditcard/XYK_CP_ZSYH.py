# -*- coding: utf-8 -*-
"""招商银行信用卡产品"""
import base64
import re
import uuid
import requests

from cha_creditcard.common import mongo_conn, mysql_conn, data_from_mongo, mysql_cursor, sql_edit, write_in_text
from cha_creditcard.config import *

COMMON_DICT = lambda: dict(
    DEAL_STATUS_="DRAFT",
    PUBLISH_STATUS_="N",
    BANK_CODE_="CMB",  # 银行编码
    BANK_NAME_="招商银行",  # 银行名称
    CREATED_BY_ID_=CREATE_BY_ID,  # 创建人ID
    CREATED_BY_NAME_=CREATE_BY_NAME,  # 创建人名称
    CREATED_TIME_=CREATE_TIME(),  # 创建时间
    MODIFIED_TIME_=CREATE_TIME()  # 修改时间
)

# Mongo 集合配置
MONGO_CONFIG["collection"] = "XYK_CP"
# MySQL 表名配置
MYSQL_CONFIG["table_name"] = "cha_creditcard_pro"
MYSQL_CONFIG["img_table_name"] = "cha_creditcard_surface"

COUNT = 0


def init_client():
    """
    初始化
    :return:
    """
    global collection
    collection = mongo_conn()
    global mysql_client
    mysql_client = mysql_conn()
    global rights_list
    rights_list = ["累积航空里程", "赠送航空意外险", "加油返现金", "取现优惠", "刷六次免次年", "校园版特权", "有效期内免年费"]
    global bz_list
    bz_list = ["双币卡", "单币卡"]
    global brand_list
    brand_list = ["银联", "VISA", "MasterCard", "JCB", "美国运通"]
    global type_list
    type_list = ["特色主题卡", "商务旅行卡", "网络/游戏卡", "卡通粉丝卡", "时尚族群卡", "其他", "NBA系列", "标准信用卡", "区域特色卡", "无限/白金卡", "商务卡系列", "校园版专区"]
    global name_list
    name_list = list()
    aggre_for_name()
    global type_dict
    type_dict = dict()


def aggre_for_name():
    agg_data = collection.aggregate([
        {"$match": {"ENTITY_CODE_": "XYK_CP_ZSYH", "DATETIME_": {'$gt': '2019-10-22'}}},
        {"$project": {"_id": 0, "NAME_": 1}},
        {"$group": {"_id": "$NAME_"}}
    ])

    global name_list
    name_list = [each["_id"] for each in agg_data]


def shuffle_data(data_list):
    return_list = list()
    data = data_list[0]
    print(data["URL_"])
    type_verify = re.findall(r"/(.*?)?WT\.mc", data["URL_"])[0]
    global type_dict
    if type_verify not in type_dict:
        type_name = re.findall(r"&name=(.*)", data["URL_"])[0]
        type_dict[type_verify] = [type_name]

        # cha_creditcard_pro
        re_data = dict()
        re_data.update(COMMON_DICT())
        # re_data["TENANT_ID_"] = ""  # 租户
        # re_data["CREATED_BY_ID_"] = ""  # 创建人ID
        # re_data["CREATED_BY_NAME_"] = ""  # 创建人名称
        # re_data["CREATED_TIME_"] = ""  # 创建时间
        # re_data["DELFLAG_"] = ""  # 逻辑删除标记
        # re_data["DISPLAY_ORDER_"] = ""  # 显示序号
        # re_data["MODIFIED_BY_ID_"] = ""  # 修改人ID
        # re_data["MODIFIED_BY_NAME_"] = ""  # 修改人名称
        # re_data[" MODIFIED_TIME_"] = CREATE_TIME()  # 修改人名称
        re_data["ID_"] = str(uuid.uuid1())  # 主键
        type_dict[type_verify].append(re_data["ID_"])
        # re_data["VERSION_"] = ""  # 版本
        # re_data["CODE_"] = ""  # 编码
        re_data["NAME_"] = type_name  # 名称
        re_data["NEW_SALE_"] = "N"  # 最新
        re_data["GOOD_SALE_"] = "N"  # 畅销
        re_data["RECOMMEND_"] = "N"  # 主推

        # for brand in brand_list:
        #     if brand in data_list:
        #         re_data["CARD_BRAND_"] = brand  # 卡组织
        #         break
        # if re_data.get("CARD_BRAND_", ""):
        #     data_list.remove(re_data["CARD_BRAND_"])

        for right in rights_list:
            if right in data_list:
                re_data["RIGHTS_INTERESTS_ID_"] = right  # 产品权益
                break
        if re_data.get("RIGHTS_INTERESTS_ID_", ""):
            data_list.remove(re_data["RIGHTS_INTERESTS_ID_"])

        for style in type_list:
            if style in data_list:
                re_data["STYLE_"] = style  # 产品权益
                break
        if re_data.get("STYLE_", ""):
            data_list.remove(re_data["STYLE_"])
        # re_data["STYLE_"] = ""  # 风格
        # re_data["LEVEL_"] = ""  # 卡等级
        # re_data["SETTLEMENT_CURRENCY_"] = ""  # 结算货币
        # if data.get("TYPE_NAME_") == "双币卡":
        #     re_data["CURRENCY_"] = "人民币 美元"  # 币种
        # else:
        #     re_data["CURRENCY_"] = "人民币"  # 币种
        # re_data["RANGE_"] = ""  # 发行范围
        # re_data["INTEREST_FREE_PERIOD_"] = ""  # 免息期
        # re_data["ANNUAL_FEE_"] = ""  # 年费
        # re_data["MINIMUM_PAYMENT_"] = ""  # 最低还款

        act_1 = re.findall(r"(\d[)）] ?[^<]+)<", data["VALUE_NAME_"])
        if act_1:
            re_data["SPECIAL_OFFER_"] = " ".join(act_1)
            # print(re_data["SPECIAL_OFFER_"])
            # print(data["VALUE_NAME_"])
        else:
            act_2 = re.findall(r"<h2class=cardfavornormalstyle=display:>(.*?)</h2>", data["VALUE_NAME_"])
            if act_2:
                re_data["SPECIAL_OFFER_"] = " ".join(act_2)
            else:
                act_3 = re.findall(r"<h2class=cardinfonormal>(.*?)</h2>", data["VALUE_NAME_"])
                if act_3:
                    re_data["SPECIAL_OFFER_"] = " ".join(act_3)
                else:
                    re_data["SPECIAL_OFFER_"] = ""

        re_data["SPECIAL_OFFER_"] = re.sub(r"<br>", " ", re_data["SPECIAL_OFFER_"])  # 优惠活动

        # re_data["VALIDITY_"] = ""  # 有效期
        # re_data["POINT_POLICY_"] = ""  # 积分政策
        # re_data["PAYMENT_POLICY_"] = ""  # 还款政策
        # re_data["CASH_POLICY_"] = ""  # 取现政策
        # re_data["APPLY_CONDITION_"] = ""  # 申请条件
        # re_data["APPLY_MATERIAL_"] = ""  # 申请资料
        # re_data["INSTALLMENT_FEE_"] = ""  # 分期手续费
        # re_data["INSTALLMENT_RATE_"] = ""  # 分期利率
        # re_data["MEDIUM_"] = ""  # 存储介质
        # re_data["TYPE_"] = ""  # 卡片关系
        # re_data["BILL_DATE_"] = ""  # 账单日
        # re_data["SUITED_CROWDED_"] = ""  # 适合人群
        # re_data["DEAL_USER_ID_"] = ""  # 处理人编码
        # re_data["DEAL_USER_NAME_"] = ""  # 处理人名称
        # re_data["DEAL_TIME_"] = ""  # 处理时间
        # re_data["DEAL_START_TIME_"] = ""  # 处理开始时间
        # re_data["DEAL_END_TIME_"] = ""  # 处理结束时间
        # re_data["PUBLISH_USER_ID_"] = ""  # 发布人编码
        # re_data["PUBLISH_USER_NAME_"] = ""  # 发布人名称
        # re_data["PUBLISH_TIME_"] = ""  # 发布时间
        # re_data["DEAL_STATUS_"] = ""  # 处理状态

        return_list.append((MYSQL_CONFIG["table_name"], re_data))

    # cha_creditcard_surface
    surface_dict = dict(COMMON_DICT())
    surface_dict["ID_"] = str(uuid.uuid1())
    surface_dict["CARD_CODE_"] = type_dict[type_verify][1]
    surface_dict["CARD_NAME_"] = type_dict[type_verify][0]
    # surface_dict["BANK_CODE_"] = data[""]
    # surface_dict["BANK_NAME_"] = data[""]
    img_response = requests.get(url=data["IMG_"])
    surface_dict["IMAGE_"] = base64.b64encode(img_response.content).decode("utf-8")
    img_response.close()
    # surface_dict["COLOR_"] = data[""]
    # surface_dict["SHAPE_"] = data[""]
    surface_dict["IS_LETTER_"] = "N"
    surface_dict["IS_CHINESE_"] = "N"
    surface_dict["IS_SYMBOL_"] = "N"
    surface_dict["JOINTLY_"] = "N"
    # surface_dict["DEFINED_TAG_"] = data[""]
    # surface_dict["APPRAISE_BY_ID_"] = data[""]
    # surface_dict["APPRAISE_BY_NAME_"] = data[""]
    # surface_dict["APPRAISE_BY_TIME_"] = data[""]
    # surface_dict["APPRAISE_CONTENT_"] = data[""]
    # surface_dict["SUITABILITY_"] = data[""]
    # surface_dict["SUITABILITY_REMARK_"] = data[""]
    # surface_dict["ARTISTRY_"] = data[""]
    # surface_dict["ARTISTRY_REMARK_"] = data[""]
    # surface_dict["RATITY_"] = data[""]
    # surface_dict["RATITY_REMARK_"] = data[""]
    # surface_dict["COLOR_MATCH_"] = data[""]
    # surface_dict["COLOR_MATCH_REMARK_"] = data[""]
    # surface_dict["DEAL_USER_ID_"] = data[""]
    # surface_dict["DEAL_USER_NAME_"] = data[""]
    # surface_dict["DEAL_TIME_"] = data[""]
    # surface_dict["DEAL_START_TIME_"] = data[""]
    # surface_dict["DEAL_END_TIME_"] = data[""]
    # surface_dict["PUBLISH_USER_ID_"] = data[""]
    # surface_dict["PUBLISH_USER_NAME_"] = data[""]
    # surface_dict["PUBLISH_TIME_"] = data[""]
    # surface_dict["TENANT_ID_"] = data[""]
    # surface_dict["CREATED_BY_ID_"] = data[""]
    # surface_dict["CREATED_BY_NAME_"] = data[""]
    # surface_dict["CREATED_TIME_"] = data[""]
    # surface_dict["DELFLAG_"] = data[""]
    # surface_dict["DISPLAY_ORDER_"] = data[""]
    # surface_dict["MODIFIED_BY_ID_"] = data[""]
    # surface_dict["MODIFIED_BY_NAME_"] = data[""]
    # surface_dict["MODIFIED_TIME_"] = data[""]
    # surface_dict["VERSION_"] = data[""]
    # surface_dict["DEAL_STATUS_"] = data[""]
    # surface_dict["PUBLISH_STATUS_"] = data[""]
    surface_dict["NAME_"] = data["NAME_"]
    # surface_dict["CURRENCY_"] = data[""]
    # surface_dict["CARD_BRAND_"] = data[""]
    for brand in brand_list:
        if brand in data_list:
            surface_dict["CARD_BRAND_"] = brand  # 卡组织
            break
    if surface_dict.get("CARD_BRAND_", ""):
        data_list.remove(surface_dict["CARD_BRAND_"])
    # surface_dict["TYPE_"] = data[""]
    return_list.append((MYSQL_CONFIG["img_table_name"], surface_dict))
    return return_list


def main():
    """
    主函数
    :return:
    """
    init_client()
    for each in name_list:
        data_list = list()
        data_iter = data_from_mongo(find_query={"$and": [
            {"ENTITY_CODE_": "XYK_CP_ZSYH"},
            {"NAME_": each},
            {"DATETIME_": {'$gt': '2019-10-22'}}
        ]},
                                    collection=collection)
        count = 0
        for data in data_iter:
            if count == 0:
                data_list.append(data)
                count += 1
            data_list.append(data["TYPE_NAME_"])

        data_list = shuffle_data(data_list)
        # print(s_data)
        sql_list = list()
        for i in data_list:
            sql = sql_edit(i[1], table_name=i[0])
            sql_list.append(sql)

        c = mysql_cursor(sql=sql_list, client=mysql_client)
        global COUNT
        COUNT += c
        print(COUNT)


if __name__ == '__main__':
    main()
