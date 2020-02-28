# -*- coding: utf-8 -*-
import hashlib
import re
import time


def data_shuffle(data):
    re_data = dict()

    # todo row_key is not defined
    hash_m = hashlib.md5()
    hash_m.update(str(data["TITLE_"]).encode("utf-8"))
    row_key = hash_m.hexdigest()
    re_data["ID_"] = row_key
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["URL_"] = data["URL_"]

    # 地区及经纬度统一在 __init_____.py 中处理
    re_data["PROVINCE_CODE_"] = ""
    re_data["PROVINCE_NAME_"] = ""
    re_data["CITY_CODE_"] = ""
    re_data["CITY_NAME_"] = ""
    re_data["AREA_CODE_"] = ""
    re_data["AREA_NAME_"] = ""
    re_data["LAT_"] = ""
    re_data["LNG_"] = ""

    # 当前为分行版
    re_data["APP_VERSION_"] = "BRANCH"

    # 涉及银行统一在 __init_____.py 中处理
    re_data["BANK_CODE_"] = ""
    re_data["BANK_NAME_"] = ""
    re_data["UNIT_CODE_"] = ""
    re_data["UNIT_NAME_"] = ""

    re_data["PERIOD_CODE_"] = data["PUBLISH_TIME_"].replace("-", "")
    # re_data["REMARK_"] = ""

    # 创建时间及操作人
    time_array = time.localtime()
    create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    re_data["CREATE_TIME_"] = create_time
    re_data["CREATE_BY_ID_"] = "P0131857"
    re_data["CREATE_BY_NAME_"] = "钟楷文"

    # 爬取时间
    re_data["SPIDER_TIME_"] = data["DATETIME_"]

    # 修改时间及操作人
    # re_data["MODIFIED_TIME_"] = data[""]
    # re_data["MODIFIED_BY_ID_"] = data[""]
    # re_data["MODIFIED_BY_NAME_"] = data[""]

    re_data["M_STATUS_"] = "NO"
    re_data["DELETE_STATUS_"] = "NO"
    re_data["DATA_STATUS_"] = "uncheck"
    re_data["PUBLISH_STATUS_"] = "NO"
    # todo 模型
    re_data["TAGS_"] = ""

    source = re.findall(r"(https?://.*?)/", data["URL_"])
    re_data["SOURCE_"] = source[0]
    re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"]
    # todo
    # re_data["SOURCE_CODE_"] = ""

    re_data["SOURCE_TYPE_"] = "CJ"

    # todo 根据模型分类
    # re_data["HOT_"] = data[""]

    # todo 业务分类
    # re_data["NEWS_TYPE1_"] = data[""]
    # re_data["NEWS_TYPE1_CODE_"] = data[""]
    # re_data["NEWS_TYPE2_"] = data[""]
    # re_data["NEWS_TYPE3_"] = data[""]

    re_data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"]
    re_data["TITLE_"] = data["TITLE_"]

    # 作者
    if "编辑" in data["AUTHOR_"]:
        re_data["AUTHOR_"] = re.findall(r"编辑[:：](\w+)", data["AUTHOR_"])[0]
    else:
        re_data["AUTHOR_"] = data["AUTHOR_"]

    # todo 模型
    # re_data["BRIEF_"] = ""

    re_data["IMPORTANCE_"] = "NO"

    # todo 模型
    # re_data["EMOTION_"] = data[""]

    # 无
    # re_data["READS_"] = data[""]
    # re_data["LIKES_"] = data[""]
    # re_data["COMMENTS_"] = data[""]
    # re_data["JOINS_"] = data[""]

    re_data["HOME_PAGE_"] = "NO"
    re_data["IMAGE_"] = data["IMAGE_"]

    # 内容
    re_data["CONTENT_"] = data["CONTENT_"]
    re_data["HTML_"] = data["HTML_"]
    re_data["ACT_"] = "NO"

    # 无
    # re_data["PDF_URL_"] = ""

    return re_data
