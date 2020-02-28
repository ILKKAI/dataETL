# -*- coding: utf-8 -*-
"""中国农业银行积分商城"""
import base64
import uuid
import requests

from cha_creditcard.common import mongo_conn, mysql_conn, mysql_cursor, data_from_mongo, sql_edit
from cha_creditcard.config import *

COMMON_DICT = lambda: dict(
    BANK_CODE_="ABC",  # 银行编码
    BANK_NAME_="中国农业银行",  # 银行名称
    # TENANT_ID_=data[""],  # 租户
    # VERSION_=data[""],  # 版本
    CREATED_BY_ID_=CREATE_BY_ID,  # 创建人ID
    CREATED_BY_NAME_=CREATE_BY_NAME,  # 创建人名称
    CREATED_TIME_=CREATE_TIME(),  # 创建时间
    # DELFLAG_=data[""],  # 逻辑删除标记
    # DISPLAY_ORDER_=data[""],  # 显示序号
    # MODIFIED_BY_ID_=data[""],  # 修改人ID
    # MODIFIED_BY_NAME_=data[""],  # 修改人名称
    MODIFIED_TIME_=CREATE_TIME()  # 修改时间
)

# Mongo 集合配置
MONGO_CONFIG["collection"] = "XYK_JFSC"
# MySQL 表名配置
MYSQL_CONFIG["table_name"] = "cha_creditcard_point_mall"
MYSQL_CONFIG["type_table_name"] = "cha_creditcard_point_mall_type"
MYSQL_CONFIG["summary_table_name"] = "cha_creditcard_point_mall_type_summary"

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


def shuffle_data(data):
    """
    清洗数据
    :param data:
    :return:
    """
    # cha_creditcard_point_mall
    re_data = dict(COMMON_DICT())
    re_data["ID_"] = str(uuid.uuid1())  # 主键

    re_data["CODE_"] = data["PRODUCT_CODE_"]  # 产品编码
    re_data["NAME_"] = data["PRODUCT_NAME_"]  # 产品名称
    re_data["TYPE_NAME_"] = data["TYPE_NAME_"]  # 产品类型名称
    re_data["TYPE_CODE_"] = data["TYPE_CODE_"]  # 产品类型编码
    img_url = "".join([data["IMG_PUB_URL_"], data["SRC_FILE_"]])
    img_response = requests.get(url=img_url)
    re_data["IMG_"] = base64.b64encode(img_response.content).decode("utf-8")
    img_response.close()

    re_data["COUNT_"] = 1  # 产品数量
    re_data["PRICE_"] = 0  # 市场价
    re_data["POINT_"] = data["POINT_"]  # 积分
    re_data["CASH_"] = 0  # 现金
    re_data["RECOMMEND_"] = "N"  # 主推商品
    re_data["GOOD_SALE_"] = "N"  # 人气爆品
    re_data["NEW_SALE_"] = "N"  # 新品推荐
    # re_data["MEMBER_"] = data[""]  # 尊享会员商城
    re_data["HTML_"] = data["DESCRIPTION_"]  # 详细内容
    # re_data["PROFILE_"] = data[""]  # 商城概况

    # # cha_creditcard_point_mall_type
    # type_dict = dict(COMMON_DICT())
    # type_dict["ID_"] = str(uuid.uuid1())  # 主键
    # # type_dict["BANK_CODE_"] = data[""]  # 银行编码
    # # type_dict["BANK_NAME_"] = data[""]  # 银行名称
    # type_dict["CODE_"] = data[""]  # 类型编码
    # type_dict["NAME_"] = data[""]  # 类型名称
    # type_dict["PARENT_"] = data[""]  # 父类编码
    # type_dict["REMAKE_"] = data[""]  # 描述
    # # type_dict["TENANT_ID_"] = data[""]  # 租户
    # # type_dict["VERSION_"] = data[""]  # 版本
    # # type_dict["CREATED_BY_ID_"] = data[""]  # 创建人ID
    # # type_dict["CREATED_BY_NAME_"] = data[""]  # 创建人名称
    # # type_dict["CREATED_TIME_"] = data[""]  # 创建时间
    # # type_dict["DELFLAG_"] = data[""]  # 逻辑删除标记
    # # type_dict["DISPLAY_ORDER_"] = data[""]  # 显示序号
    # # type_dict["MODIFIED_BY_ID_"] = data[""]  # 修改人ID
    # # type_dict["MODIFIED_BY_NAME_"] = data[""]  # 修改人名称
    # # type_dict["MODIFIED_TIME_"] = data[""]  # 修改时间
    #
    # # cha_creditcard_point_mall_type_summary
    # summary_dict = dict(COMMON_DICT())
    # summary_dict["ID_"] = data[""]  # 主键
    # # summary_dict["BANK_CODE_"] = data[""]  # 银行编码
    # # summary_dict["BANK_NAME_"] = data[""]  # 银行名称
    # summary_dict["PRICE_RATE_"] = data[""]  # 总体性价比
    # summary_dict["CODE_"] = data[""]  # 类型编码 银行编码+编码
    # summary_dict["NAME_"] = data[""]  # 类型名称
    # summary_dict["NUM_"] = data[""]  # 数量
    # summary_dict["PERF_PRICE_RATE_"] = data[""]  # 类别性价比
    # # summary_dict["TENANT_ID_"] = data[""]  # 租户
    # # summary_dict["VERSION_"] = data[""]  # 版本
    # # summary_dict["CREATE    D_BY_ID_
    # "] = data[""]  # 创建人ID
    # # summary_dict["CREATED_BY_NAME_"] = data[""]  # 创建人名称
    # # summary_dict["CREATEG_"] = data[""]  # 逻辑删除标记
    # # summary_dict["DISPLD_TIME_"] = data[""]  # 创建时间
    # # summary_dict["DELFLAAY_ORDER_"] = data[""]  # 显示序号
    # # summary_dict["MODIFIED_BY_ID_"] = data[""]  # 修改人ID
    # # summary_dict["MODIFIED_BY_NAME_"] = data[""]  # 修改人名称
    # # summary_dict["MODIFIED_TIME_"] = data[""]  # 修改时间

    return [
        (MYSQL_CONFIG["table_name"], re_data),
        # (MYSQL_CONFIG["type_table_name"], type_dict),
        # (MYSQL_CONFIG["summary_table_name"], summary_dict)
    ]


def main():
    init_client()
    data_iter = data_from_mongo(find_query={}, collection=collection)
    sql_list = list()
    for data in data_iter:
        data_list = shuffle_data(data)
        for i in data_list:
            sql = sql_edit(i[1], table_name=i[0])
            sql_list.append(sql)
            global COUNT
            COUNT += 1
    count = mysql_cursor(sql=sql_list, client=mysql_client)
    print(count)


if __name__ == '__main__':
    main()
