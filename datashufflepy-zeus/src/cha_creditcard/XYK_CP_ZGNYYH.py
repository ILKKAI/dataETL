# -*- coding: utf-8 -*-
"""中国农业银行信用卡产品"""
import base64
import re
import uuid
import requests

from cha_creditcard.common import mongo_conn, mysql_conn, data_from_mongo, mysql_cursor, sql_edit
from cha_creditcard.config import *

COMMON_DICT = lambda: dict(
    DEAL_STATUS_="DRAFT",
    PUBLISH_STATUS_="N",
    BANK_CODE_="ABC",  # 银行编码
    BANK_NAME_="中国农业银行",  # 银行名称
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

    aggre_for_name()


def aggre_for_name():
    agg_data = collection.aggregate([
        {"$match": {"ENTITY_CODE_": "XYK_CP_ZGNYYH", "DATETIME_": {'$gt': '2019-10-22'}}},
        {"$project": {"_id": 0, "NAME_": 1}},
        {"$group": {"_id": "$NAME_"}}
    ])

    global name_list
    name_list = [each["_id"] for each in agg_data]


def disting_table(data_iter):
    count = 0
    data_list = list()
    for data in data_iter:
        re_list = shuffle_data(data, count)
        data_list.extend(re_list)
        count += 1
    return data_list


def shuffle_data(data, count):
    re_list = list()
    if count == 0:
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
        # re_data[" MODIFIED_TIME_="] = CREATE_TIME()  # 修改人名称

        re_data["ID_"] = str(uuid.uuid1())  # 主键
        global card_id
        card_id = re_data["ID_"]
        # re_data["VERSION_"] = ""  # 版本
        # re_data["CODE_"] = ""  # 编码
        re_data["NAME_"] = data.get('NAME_') if data.get('NAME_') else data['CARD_NAME_']  # 名称
        global card_name
        card_name = re_data["NAME_"]
        re_data["NEW_SALE_"] = "N"  # 最新
        re_data["GOOD_SALE_"] = "N"  # 畅销
        re_data["RECOMMEND_"] = "N"  # 主推
        # if "银联" in data["CARD_NAME_"]:
        #     re_data["CARD_BRAND_"] = "银联"  # 卡组织
        # elif "万事达" in data["CARD_NAME_"]:
        #     re_data["CARD_BRAND_"] = "VISA"  # 卡组织
        # re_data["RIGHTS_INTERESTS_ID_"] = right  # 产品权益
        # re_data["STYLE_"] = ""  # 风格
        # re_data["LEVEL_"] = ""  # 卡等级
        # re_data["SETTLEMENT_CURRENCY_"] = ""  # 结算货币
        # re_data["CURRENCY_"] = data["CURRENCY_"]  # 币种

        # re_data["RANGE_"] = ""  # 发行范围
        re_data["INTEREST_FREE_PERIOD_"] = data["INTEREST_FREE_PERIOD_"]  # 免息期
        # re_data["ANNUAL_FEE_"] = data["ANNUAL_FEE_"]  # 年费
        # re_data["MINIMUM_PAYMENT_"] = ""  # 最低还款

        re_data["SPECIAL_OFFER_"] = data["TITLE_"].replace("|", "、")  # 优惠活动

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
        re_data["BILL_DATE_"] = data["BILL_DATE_"]  # 账单日
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
        re_list.append((MYSQL_CONFIG["table_name"], re_data))

    # cha_creditcard_surface
    surface_dict = dict(COMMON_DICT())
    surface_dict["ID_"] = str(uuid.uuid1())  # 主键
    surface_dict["CARD_CODE_"] = card_id  # 信用卡编码
    surface_dict["CARD_NAME_"] = card_name  # 信用卡名称
    # surface_dict["BANK_CODE_"] = data[""]  # 银行编码
    # surface_dict["BANK_NAME_"] = data[""]  # 银行名称
    img_url = "".join([data["IMG_PUB_URL_"], data["SRC_FILE_"]])
    img_response = requests.get(url=img_url)
    surface_dict["IMAGE_"] = base64.b64encode(img_response.content).decode("utf-8")  # 卡面图片
    img_response.close()
    # surface_dict["COLOR_"] = data[""]  # 卡面颜色
    # surface_dict["SHAPE_"] = data[""]  # 卡面形状
    surface_dict["IS_LETTER_"] = "N"  # 卡面字母
    surface_dict["IS_CHINESE_"] = "N"  # 卡面汉字
    surface_dict["IS_SYMBOL_"] = "N"  # 卡面图形符号
    surface_dict["JOINTLY_"] = "N"  # 是否为联名卡
    # surface_dict["DEFINED_TAG_"] = data[""]  # 自定义标签
    # surface_dict["APPRAISE_BY_ID_"] = data[""]  # 卡面分析人员编码
    # surface_dict["APPRAISE_BY_NAME_"] = data[""]  # 卡面分析人员姓名
    # surface_dict["APPRAISE_BY_TIME_"] = data[""]  # 卡面分析时间
    # surface_dict["APPRAISE_CONTENT_"] = data[""]  # 卡面分析内容
    # surface_dict["SUITABILITY_"] = data[""]  # 适用性
    # surface_dict["SUITABILITY_REMARK_"] = data[""]  # 适用性评价备注
    # surface_dict["ARTISTRY_"] = data[""]  # 艺术性
    # surface_dict["ARTISTRY_REMARK_"] = data[""]  # 艺术性评价备注
    # surface_dict["RATITY_"] = data[""]  # 稀有性
    # surface_dict["RATITY_REMARK_"] = data[""]  # 稀有性评价备注
    # surface_dict["COLOR_MATCH_"] = data[""]  # 色彩搭配
    # surface_dict["COLOR_MATCH_REMARK_"] = data[""]  # 色彩搭配评价备注
    # surface_dict["DEAL_USER_ID_"] = data[""]  # 处理人编码
    # surface_dict["DEAL_USER_NAME_"] = data[""]  # 处理人名称
    # surface_dict["DEAL_TIME_"] = data[""]  # 处理时间
    # surface_dict["DEAL_START_TIME_"] = data[""]  # 处理开始时间
    # surface_dict["DEAL_END_TIME_"] = data[""]  # 处理结束时间
    # surface_dict["PUBLISH_USER_ID_"] = data[""]  # 发布人编码
    # surface_dict["PUBLISH_USER_NAME_"] = data[""]  # 发布人名称
    # surface_dict["PUBLISH_TIME_"] = data[""]  # 发布时间
    # surface_dict["TENANT_ID_"] = data[""]  # 租户
    # surface_dict["CREATED_BY_ID_"] = data[""]  # 创建人ID
    # surface_dict["CREATED_BY_NAME_"] = data[""]  # 创建人名称
    # surface_dict["CREATED_TIME_"] = data[""]  # 创建时间
    # surface_dict["DELFLAG_"] = data[""]  # 逻辑删除标记
    # surface_dict["DISPLAY_ORDER_"] = data[""]  # 显示序号
    # surface_dict["MODIFIED_BY_ID_"] = data[""]  # 修改人ID
    # surface_dict["MODIFIED_BY_NAME_"] = data[""]  # 修改人名称
    # surface_dict["MODIFIED_TIME_"] = data[""]  # 修改时间
    # surface_dict["VERSION_"] = data[""]  # 版本
    # surface_dict["DEAL_STATUS_"] = data[""]  # 处理状态
    # surface_dict["PUBLISH_STATUS_"] = data[""]  # 发布状态
    surface_dict["NAME_"] = data["CARD_NAME_"]  # 卡面名称
    surface_dict["CURRENCY_"] = data["CURRENCY_"]  # 卡币种
    # surface_dict["CARD_BRAND_"] = re_data.get("CARD_BRAND_")  # 卡组织
    if "银联" in data["CARD_NAME_"]:
        surface_dict["CARD_BRAND_"] = "银联"  # 卡组织
    if "万事达" in data["CARD_NAME_"]:
        if "CARD_BRAND_" in surface_dict:
            surface_dict["CARD_BRAND_"] = surface_dict["CARD_BRAND_"] + "&万事达"  # 卡组织
        else:
            surface_dict["CARD_BRAND_"] = "万事达"  # 卡组织
    if "Visa" in data["CARD_NAME_"]:
        if "CARD_BRAND_" in surface_dict:
            surface_dict["CARD_BRAND_"] = surface_dict["CARD_BRAND_"] + "&VISA"  # 卡组织
        else:
            surface_dict["CARD_BRAND_"] = "VISA"  # 卡组织

    # surface_dict["TYPE_"] = data[""]  # 卡片关系

    surface_dict["ANNUAL_FEE_"] = data["ANNUAL_FEE_"]  # 年费
    # return [(MYSQL_CONFIG["table_name"], re_data), (MYSQL_CONFIG["img_table_name"], surface_dict)]
    re_list.append((MYSQL_CONFIG["img_table_name"], surface_dict))
    return re_list


def main():
    """
    主函数
    :return:
    """
    init_client()
    for name in name_list:
        data_iter = data_from_mongo(find_query={"$and": [
            {"ENTITY_CODE_": "XYK_CP_ZGNYYH"},
            {"NAME_": name},
            {"DATETIME_": {'$gt': '2019-10-22'}}
        ]}, collection=collection)

        data_list = disting_table(data_iter)
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
