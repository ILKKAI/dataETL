# -*- coding: utf-8 -*-
import random

import arrow

from database._phoenix_hbase import *


class Script(object):
    def __init__(self, table_name):
        self.script_name = table_name
        self.p_client = PhoenixHbase(table_name=table_name)
        # self.p_client = PhoenixHbase(table_name="WEIBOINFO")
        self.connection = self.p_client.connect_to_phoenix()
        self.weibo_code = {'中国农业银行': '1775158187', '中国银行': '2242603603', '交行微银行': '2100521990', '渤海银行电子银行': '3818803465', '建行电子银行': '1803278047', '中国光大银行': '1698631892', '广发银行': '2178593782', '兴业银行': '1940411570', '民生银行手机银行': '2384005224', '招商银行': '1653150224', '恒丰银行': '5084120819', '中信银行': '2179623947', '中国工商银行': '5132862451', '邮储银行电子银行': '3887966172', '浦发银行': '2012572483'}

    # 新闻资讯
    def NEWS_FINASSIST(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "NO"
        copy_result["TAGS_"] = result["KEYWORDS_"]
        source = re.findall(r"(https?://.*?)/", result["URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["SOURCE_CODE_"] = ""
        copy_result["SOURCE_TYPE_"] = "CJ"
        # copy_result["HOT_"] = result[""]
        # copy_result["NEWS_TYPE1_"] = result[""]
        # copy_result["NEWS_TYPE2_"] = result[""]
        # copy_result["NEWS_TYPE3_"] = result[""]
        copy_result["PUBLISH_TIME_"] = result["PUBLISH_TIME_"]
        copy_result["TITLE_"] = result["TITLE_"]
        # copy_result["AUTHOR_"] = result[""]
        copy_result["BRIEF_"] = result["BRIEF_"]
        copy_result["IMPORTANCE_"] = "NO"
        # copy_result["EMOTION_"] = result[""]
        # copy_result["READS_"] = result[""]
        # copy_result["LIKES_"] = result[""]
        # copy_result["COMMENTS_"] = result[""]
        # copy_result["JOINS_"] = result[""]
        copy_result["HOME_PAGE_"] = "NO"
        # copy_result["IMAGE_"] = result[""]
        copy_result["CONTENT_"] = result["CONTENT_"]
        # copy_result["HTML_"] = result[""]
        copy_result["ACT_"] = "NO"

        self.p_client.table_name = "CHA_BRANCH_NEWS"

        # fake data
        copy_result["PROVINCE_CODE_"] = "0000"
        copy_result["PROVINCE_NAME_"] = "中国(省)"
        copy_result["CITY_CODE_"] = "000000"
        copy_result["CITY_NAME_"] = "中国(市)"
        copy_result["AREA_CODE_"] = "000001"
        copy_result["AREA_NAME_"] = "中国(区县)"
        copy_result["LAT_"] = "37.5503394745908"
        copy_result["LNG_"] = "104.11412925347894"
        copy_result["UNIT_CODE_"] = "00000"
        copy_result["UNIT_NAME_"] = "中国(省市)"

        copy_result["TAGS_"] = ["银行", "新闻", "交易", "维护", "暂停", "升级"][random.randint(0, 5)]
        copy_result["AUTHOR_"] = "XXX"
        copy_result["EMOTION_"] = ["POSITIVE", "NORMAL", "NAGATIVE"][random.randint(0, 2)]
        copy_result["READS_"] = random.randint(1, 10000)
        copy_result["LIKES_"] = random.randint(1, 10000)
        copy_result["COMMENTS_"] = random.randint(1, 10000)
        copy_result["JOINS_"] = random.randint(1, 10000)

        source_code_list = ["ZX_HYBG_BMW", "ZX_HYBG_PHYD", "ZX_HYBG_DQ", "ZX_HYBG_MKX", "ZX_HYBG_ARZX", "ZX_HYBG_YDGXT",
                            "ZX_HYBG_ITJZ", "ZX_HYBG_YGZK", "ZX_HYBG_YOZK", "ZX_ZCGG_SJS", "ZX_ZCGG_SZJS",
                            "ZX_ZCGG_YBH", "ZX_ZCGG_BJH", "ZX_ZCGG_YJH", "ZX_ZCGG_ZGRMYH"]
        copy_result["SOURCE_CODE_"] = source_code_list[random.randint(0, 14)]
        news_tc_list = ["JRCP", "GPZQ", "HLWJR", "JRKJ", "FDC", "ZCFG", "QT", "HDDT", "QYWH", "PHJR", "QYJS", "HYCP",
                        "JRKJ", "YHNB", "HJDT"]
        copy_result["NEWS_TYPE1_CODE_"] = news_tc_list[random.randint(0, 14)]
        copy_result["HOT_"] = random.randint(1, 5)

        return copy_result

    # 官网新闻
    def GWNEWS_FINASSIST(self, result):
        print(result)
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        # copy_result["TAGS_"] = result["KEYWORDS_"]
        source = re.findall(r"(https?://.*?)/", result["URL_"])
        print(result["URL_"])
        print(result)
        if source:
            copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = result["ENTITY_NAME_"]
        copy_result["SOURCE_TYPE_"] = "GFDT"
        # copy_result["HOT_"] = result[""]
        # copy_result["NEWS_TYPE1_"] = result[""]
        # copy_result["NEWS_TYPE2_"] = result[""]
        # copy_result["NEWS_TYPE3_"] = result[""]
        copy_result["PUBLISH_TIME_"] = result["PUBLISH_TIME_"]
        copy_result["TITLE_"] = result["TITLE_"]
        # copy_result["AUTHOR_"] = result[""]
        # copy_result["BRIEF_"] = result["BRIEF_"]
        copy_result["IMPORTANCE_"] = "NO"
        # copy_result["EMOTION_"] = result[""]
        # copy_result["READS_"] = result[""]
        # copy_result["LIKES_"] = result[""]
        # copy_result["COMMENTS_"] = result[""]
        # copy_result["JOINS_"] = result[""]
        copy_result["HOME_PAGE_"] = "NO"
        # copy_result["IMAGE_"] = result[""]
        copy_result["CONTENT_"] = result["CONTENT_"]
        # copy_result["HTML_"] = result[""]
        copy_result["ACT_"] = "NO"

        self.p_client.table_name = "CHA_BRANCH_NEWS"

        # fake data
        copy_result["PROVINCE_CODE_"] = "0000"
        copy_result["PROVINCE_NAME_"] = "中国(省)"
        copy_result["CITY_CODE_"] = "000000"
        copy_result["CITY_NAME_"] = "中国(市)"
        copy_result["AREA_CODE_"] = "000001"
        copy_result["AREA_NAME_"] = "中国(区县)"
        copy_result["LAT_"] = "37.5503394745908"
        copy_result["LNG_"] = "104.11412925347894"
        copy_result["UNIT_CODE_"] = "00000"
        copy_result["UNIT_NAME_"] = "中国(省市)"

        copy_result["BRIEF_"] = "简介:" + result["TITLE_"]
        copy_result["TAGS_"] = ["银行", "新闻", "交易", "维护", "暂停", "升级"][random.randint(0, 5)]
        copy_result["AUTHOR_"] = "XXX"
        copy_result["EMOTION_"] = ["POSITIVE", "NORMAL", "NAGATIVE"][random.randint(0, 2)]
        copy_result["READS_"] = random.randint(1, 10000)
        copy_result["LIKES_"] = random.randint(1, 10000)
        copy_result["COMMENTS_"] = random.randint(1, 10000)
        copy_result["JOINS_"] = random.randint(1, 10000)

        source_code_list = ["ZX_HYBG_BMW", "ZX_HYBG_PHYD", "ZX_HYBG_DQ", "ZX_HYBG_MKX", "ZX_HYBG_ARZX", "ZX_HYBG_YDGXT",
                            "ZX_HYBG_ITJZ", "ZX_HYBG_YGZK", "ZX_HYBG_YOZK", "ZX_ZCGG_SJS", "ZX_ZCGG_SZJS",
                            "ZX_ZCGG_YBH", "ZX_ZCGG_BJH", "ZX_ZCGG_YJH", "ZX_ZCGG_ZGRMYH"]
        copy_result["SOURCE_CODE_"] = source_code_list[random.randint(0, 14)]
        news_tc_list = ["JRCP", "GPZQ", "HLWJR", "JRKJ", "FDC", "ZCFG", "QT", "HDDT", "QYWH", "PHJR", "QYJS", "HYCP",
                        "JRKJ", "YHNB", "HJDT"]
        copy_result["NEWS_TYPE1_CODE_"] = news_tc_list[random.randint(0, 14)]
        copy_result["HOT_"] = random.randint(1, 5)

        return copy_result

    # 微信
    def WECHAT(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "NO"
        # copy_result["TAGS_"] = result[""]
        # source = re.findall(r"(https?://.*?)/", result["URL_"])
        # copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = result[""]
        # copy_result["HOT_"] = result[""]
        copy_result["PUBLISH_TIME_"] = result["NOTICE_TIME_"]
        copy_result["TITLE_"] = result["TITLE_"]
        copy_result["WECHAT_ID_"] = result["WECHAT_ID_"]
        copy_result["WECHAT_NAME_"] = result["WECHAT_NAME_"]
        copy_result["IMPORTANCE_"] = "NO"
        copy_result["READS_"] = result["READ_NUM_"]
        # copy_result["LIKES_"] = result[""]
        # copy_result["COMMENTS_"] = result[""]
        copy_result["ACT_"] = "NO"
        # copy_result["ACT_TYPE_"] = result[""]
        # content = re.findall(r"[\u4e00-\u9fa5]+", result["CONTENT_"])
        # copy_result["CONTENT_"] = "".join(content)
        copy_result["HTML_"] = result["CONTENT_"]
        # copy_result["TYPE_"] = result[""]

        self.p_client.table_name = "CHA_BRANCH_WECHAT"

        # fake data
        if not copy_result["READS_"]:
            copy_result["READS_"] = random.randint(1, 10000)
        copy_result["LIKES_"] = random.randint(1, 10000)
        copy_result["COMMENTS_"] = random.randint(1, 10000)
        content = re.findall(r"[\u4e00-\u9fa5]+", result["CONTENT_"])
        copy_result["CONTENT_"] = "".join(content)
        copy_result["TYPE_"] = ["公告类", "股市行情", "政策法规动态", "企业文化", "业务/产品动态", "营销活动", "其他"][random.randint(0, 6)]
        copy_result["TYPE_CODE_"] = ["GGL", "GSHQ", "ZCFGDT", "QYWH", "YW_CPDT", "YXHD", "QT"][random.randint(0, 6)]
        copy_result["HOT_"] = random.randint(1, 5)
        return copy_result

    # 微博基本信息
    def WEIBOBASICINFO(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        copy_result["URL_"] = result["COMPANY_URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", result["COMPANY_URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = result[""]
        # copy_result["HOT_"] = result[""]
        copy_result["WEIBO_CODE_"] = result["WEIBO_CODE_"]
        copy_result["WEIBO_NAME_"] = result["NAME_"]
        copy_result["FOCUS_"] = result["FOCUS_"]
        copy_result["FANS_"] = result["FANS_"]
        copy_result["COMPANY_"] = result["COMPANY_"]
        copy_result["VIRIFIED_"] = result["VIRIFIED_"]
        copy_result["BRIEF_"] = result["BIREF_"]

        self.p_client.table_name = "CHA_BRANCH_WEIBO_BASIC"

        # fake data
        copy_result["HOT_"] = random.randint(1, 5)
        return copy_result

    # 微博
    def WEIBOINFO(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        copy_result["WEIBO_NAME_"] = result["ENTITY_NAME_"][:-2]
        copy_result["WEIBO_CODE_"] = self.weibo_code[copy_result["WEIBO_NAME_"]]

        copy_result["URL_"] = result["CONTENT_URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "NO"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", result["CONTENT_URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = result[""]
        # copy_result["HOT_"] = result[""]
        # copy_result["BRIEF_"] = result[""]
        copy_result["LIKES_"] = result["PRAISES_"]
        copy_result["COMMENTS_"] = result["REPLIES_"]
        copy_result["RELAYS_"] = result["RELAYS_"]
        # copy_result["ACT_"] = "NO"
        # copy_result["ACT_TYPE_"] = result[""]
        copy_result["IMPORTANCE_"] = "NO"
        copy_result["PUBLISH_TIME_"] = result["NOTICE_TIME_"]
        # copy_result["IMAGES_"] = result["CONTENT_IMAGES_"]
        copy_result["CONTENT_"] = result["CONTENT_"]
        # copy_result["TYPE_"] = result[""]

        self.p_client.table_name = "CHA_BRANCH_WEIBO_INFO"
        # fake data
        copy_result["TYPE_CODE_"] = ["GGL", "GSHQ", "ZCFGDT", "QYWH", "YW_CPDT", "YXHD", "QT"][random.randint(0, 6)]
        return copy_result

    # 微博评论
    def WEIBO_COMMENT(self, result):
        print(result)
        copy_list = list()
        self.p_client.table_name = "CHA_BRANCH_WEIBO_COMMENT"
        try:
            info_list = eval(result["INFO_COMMENTS_"])
        except Exception:
            return None
        if info_list:
            for each in info_list:
                copy_data = dict()
                copy_data["ID_"] = result["ID_"]
                copy_data["INFO_ID_"] = self.weibo_code[result["ENTITY_NAME_"][:-2]]
                copy_data["COMMENT_"] = each["COMMENT_"]
                copy_data["REPLIER_TIME_"] = each["REPLIER_TIME_"]
                copy_data["REPLIER_HEAD_"] = each["REPLIER_HEAD_"]
                copy_data["REPLIER_PRAISES_"] = each["REPLIER_PRAISES_"]
                copy_data["REPLIER_"] = each["REPLIER_"]
                copy_data["REPLIER_REPLIES_"] = each["REPLIER_REPLIES_"]
                # copy_data["EMOTION_"] = result[""]
                # copy_data["SENSITIVE_WORD_"] = result[""]
                # copy_data["SENSITIVE_"] = result[""]
                # copy_data["VERSION_"] = result[""]
                copy_data["CREATE_BY_ID_"] = "P0131857"
                copy_data["CREATE_BY_NAME_"] = "钟楷文"
                # copy_data["MODIFIED_BY_ID_"] = result[""]
                # copy_data["MODIFIED_BY_NAME_"] = result[""]
                copy_list.append(copy_data)
            return copy_list
        else:
            return None

    # 营销活动原表无数据
    def MARKETING_ACT(self, copy_result):
        # # old data
        # copy_result = dict()
        # copy_result["ID_"] = row_key
        # copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        # copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["URL_"] = result["URL_"]
        # # copy_result["PROVINCE_CODE_"] = result[""]
        # # copy_result["PROVINCE_NAME_"] = result[""]
        # # copy_result["CITY_CODE_"] = result[""]
        # # copy_result["CITY_NAME_"] = result[""]
        # # copy_result["AREA_CODE_"] = result[""]
        # # copy_result["AREA_NAME_"] = result[""]
        # # copy_result["LAT_"] = result[""]
        # # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        # copy_result["BANK_CODE_"] = result["ENTITY_CODE_"].replace("PRIVATEINFO", "")
        # copy_result["BANK_NAME_"] = result["ENTITY_NAME_"].replace("私行动态", "")
        # # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # # copy_result["UNIT_NAME_"] = result[""]
        # copy_result["PERIOD_CODE_"] = result["NOTICE_TIME_"].replace("-", "")
        # # copy_result["REMARK_"] = result[""]
        # time_array = time.localtime()
        # create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        # copy_result["CREATE_TIME_"] = create_time
        # copy_result["SPIDER_TIME_"] = result["DATETIME_"]
        # # copy_result["MODIFIED_TIME_"] = result[""]
        # copy_result["CREATE_BY_ID_"] = "P0131857"
        # copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # # copy_result["MODIFIED_BY_ID_"] = result[""]
        # # copy_result["MODIFIED_BY_NAME_"] = result[""]
        # copy_result["M_STATUS_"] = "0"
        # copy_result["DELETE_STATUS_"] = "0"
        # copy_result["DATA_STATUS_"] = "uncheck"
        # # copy_result["TAGS_"] = result[""]
        # source = re.findall(r"(https?://.*?)/", result["URL_"])
        # copy_result["SOURCE_"] = source[0]
        # copy_result["SOURCE_NAME_"] = data["ENTITY_NAME_"]
        # # copy_result["SOURCE_TYPE_"] = result[""]
        # # copy_result["HOT_"] = result[""]
        # # copy_result["IMPORTANCE_"] = result[""]
        # copy_result["ACT_NAME_"] = result["TITLE_"]
        # # copy_result["IMAGES_"] = result[""]
        # # copy_result["TARGET_"] = result[""]
        # # copy_result["BRIEF_"] = result[""]
        # copy_result["DETAILS_"] = result["CONTENT_"]
        # # copy_result["RULE_"] = result[""]
        # # copy_result["START_TIME_"] = result[""]
        # # copy_result["END_TIME_"] = result[""]
        # # copy_result["ACT_TYPE1_"] = result[""]
        # # copy_result["ACT_TYPE2_"] = result[""]
        # # copy_result["ACT_TYPE3_"] = result[""]
        # copy_result["PUBLISH_TIME_"] = result["NOTICE_TIME_"]
        # # copy_result["READS_"] = result[""]
        # # copy_result["LIKES_"] = result[""]
        # # copy_result["COMMENTS_"] = result[""]
        # # copy_result["JOINS_"] = result[""]
        # # copy_result["RELAYS_"] = result[""]
        # # copy_result["SOURCE_ID_"] = result[""]
        # # copy_result["HTML_"] = result[""]
        # # copy_result["SOURCE_OWN_NAME_"] = result[""]
        # # copy_result["SOURCE_OWN_ID_"] = result[""]

        # fake data
        copy_result["PROVINCE_CODE_"] = "0000"
        copy_result["PROVINCE_NAME_"] = "中国(省)"
        copy_result["CITY_CODE_"] = "000000"
        copy_result["CITY_NAME_"] = "中国(市)"
        copy_result["AREA_CODE_"] = "000001"
        copy_result["AREA_NAME_"] = "中国(区县)"
        copy_result["LAT_"] = "37.5503394745908"
        copy_result["LNG_"] = "104.11412925347894"
        if not copy_result["UNIT_CODE_"]:
            copy_result["UNIT_CODE_"] = "00000"
            copy_result["UNIT_NAME_"] = "中国(省市)"
        if (not copy_result["TAGS_"]) and (copy_result["ACT_NAME_"]):
            tags = re.findall(r"扶贫|中国银行|中银|会面|暂停|私人银行|大会|年会|建设银行|建行|服务", copy_result["ACT_NAME_"])
            print(tags)
            copy_result["TAGS_"] = "|".join(tags)
        if copy_result["ACT_NAME_"]:
            copy_result["BRIEF_"] = "简介:" + copy_result["ACT_NAME_"]
        if not copy_result["START_TIME_"]:
            at_array = arrow.get(copy_result["PUBLISH_TIME_"], "YYYY-MM-DD")
            time1 = at_array.shift(days=3)
            start_time = time1.format("YYYY-MM-DD")
            copy_result["START_TIME_"] = start_time
            time2 = at_array.shift(days=10)
            end_time = time2.format("YYYY-MM-DD")
            copy_result["END_TIME_"] = end_time
        copy_result["READS_"] = random.randint(1, 10000)
        copy_result["LIKES_"] = random.randint(1, 10000)
        copy_result["COMMENTS_"] = random.randint(1, 10000)
        copy_result["JOINS_"] = random.randint(1, 10000)
        copy_result["RELAYS_"] = random.randint(1, 10000)

        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["PUBLISH_STATUS_"] = "NO"
        source_dict = {"微信": "WX", "微博": "WB", "官网": "GW", "信用卡活动": "XYK"}
        copy_result["SOURCE_TYPE_"] = ["微信", "微博", "官网", "信用卡活动"][random.randint(0, 3)]
        copy_result["SOURCE_TYPE_CODE_"] = source_dict[copy_result["SOURCE_TYPE_"]]
        copy_result["HOT_"] = random.randint(1, 5)
        copy_result["IMPORTANCE_"] = "NO"
        act_dict = {"税务法律": "SWFL", "子女教育": "ZNJY", "健康医养": "JKYY", "财富管理": "CFGL", "生活娱乐": "SHYL",
                    "旅游出行": "LXCX", "艺术/艺术品": "YS_YSP", "节日庆贺": "JRQZ", "信用卡活动": "XYKHD", "其他": "QT"}
        copy_result["ACT_TYPE1_"] = ["税务法律", "子女教育", "健康医养", "财富管理", "生活娱乐", "旅游出行", "艺术/艺术品", "节日庆贺", "信用卡活动", "其他"][
            random.randint(0, 9)]
        copy_result["ACT_TYPE1_CODE_"] = act_dict[copy_result["ACT_TYPE1_"]]

        return copy_result

    # 理财产品
    def FINPRODUCT_FINASSIST(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "N"
        copy_result["DELETE_STATUS_"] = "N"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "N"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", result["URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = result["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = result[""]
        # copy_result["HOT_"] = result[""]
        copy_result["PRO_NAME_"] = result["NAME_"]
        copy_result["PRO_ORG_"] = result["BANK_NAME_"]
        copy_result["PRO_CODE_"] = result["CODE_"]
        copy_result["PRO_STATUS_"] = result["SALE_STATUS_"]
        copy_result["OPT_MODE_"] = result["OPERA_MODEL_"]
        copy_result["YIELD_TYPE_"] = result["YIELD_TYPE_"]
        copy_result["YIELD_TYPE_CODE_"] = result["YIELD_TYPE_CODE_"]
        copy_result["CURRENCY_TYPE_"] = result["CURRENCY_TYPE_"]
        # copy_result["CURRENCY_TYPE_CODE_"] = result[""]
        copy_result["START_FUNDS_"] = result["START_FUNDS_"]

        if int(copy_result["START_FUNDS_"]) <= 10000:
            copy_result["START_FUNDS_CODE_"] = "S0_1"
        elif 10000 < int(copy_result["START_FUNDS_"]) <= 50000:
            copy_result["START_FUNDS_CODE_"] = "S1_5"
        elif 50000 < int(copy_result["START_FUNDS_"]) <= 100000:
            copy_result["START_FUNDS_CODE_"] = "S5_10"
        else:
            copy_result["START_FUNDS_CODE_"] = "S10_"

        # copy_result["RISK_LEVEL_"] = result[""]
        # copy_result["RISK_LEVEL_CODE_"] = result[""]
        copy_result["SOURCE_RISK_LEVEL_"] = result["RISK_LEVEL_"]
        copy_result["RAISE_START_"] = result["SALE_START_"]
        copy_result["RAISE_END_"] = result["SALE_END_"]
        copy_result["PRO_START_"] = result["YIELD_START_DATE_"]
        copy_result["PRO_END_"] = result["YIELD_END_DATE_"]
        copy_result["YIELD_LOW_"] = result["LOW_YIELD_RATE_"]

        copy_result["YIELD_HIGH_"] = result["HIGH_YIELD_RATE_"]
        if float(copy_result["YIELD_HIGH_"]) <= 3.5:
            copy_result["YIELD_HIGH_CODE_"] = "Y1_3.5"
        elif 3.5 < float(copy_result["YIELD_HIGH_"]) <= 4.5:
            copy_result["YIELD_HIGH_CODE_"] = "Y3.5_4.5"
        elif 4.5 < float(copy_result["YIELD_HIGH_"]):
            copy_result["YIELD_HIGH_CODE_"] = "Y4.5_"

        copy_result["REAL_DAYS_"] = result["INVEST_PERIOD_"]
        if int(copy_result["REAL_DAYS_"]) <= 90:
            copy_result["REAL_DAYS_CODE_"] = "D1_3"
        elif 90 < int(copy_result["REAL_DAYS_"]) <= 180:
            copy_result["REAL_DAYS_CODE_"] = "D3_6"
        elif 180 < int(copy_result["REAL_DAYS_"]) <= 366:
            copy_result["REAL_DAYS_CODE_"] = "D6_12"
        elif 366 < int(copy_result["REAL_DAYS_"]):
            copy_result["REAL_DAYS_CODE_"] = "D12_"

        copy_result["TARGET_TYPE_"] = result["TARGET_"]
        copy_result["SALE_AREA_"] = result["SALE_DISTRICT_"]
        # copy_result["REDEEM_"] = result[""]
        copy_result["INCREASE_"] = result["INCREASE_UNIT_"]
        copy_result["INVEST_RANGE_"] = result["INVEST_RANGE_"]
        copy_result["RECOMMEND_"] = "N"
        copy_result["GOOD_SALE_"] = "N"
        copy_result["NEW_SALE_"] = "N"
        copy_result["SALE_SOURCE_"] = "NET"
        # copy_result["PDF_"] = result[""]
        # copy_result[""] = result[""]

        # fake data
        prov_list = list()
        prov_code_list = list()
        city_list = list()
        city_code_list = list()
        lat_list = list()
        lng_list = list()
        for city in self.city_list:
            if city["NAME_"][:2] in copy_result["SALE_AREA_"]:
                city_list.append(city["NAME_"])
                city_code_list.append(city["CODE_"])
                prov_code_list.append(city["PARENT_"])
                lat_list.append(city["LAT_"])
                lng_list.append(city["LNG_"])
        for prov in self.province_list:
            if prov["NAME_"][:2] in copy_result["SALE_AREA_"]:
                prov_list.append(prov["NAME_"])
                prov_code_list.append(prov["CODE_"])
                lat_list.append(prov["LAT_"])
                lng_list.append(prov["LNG_"])
            elif prov["CODE_"] in prov_code_list:
                prov_list.append(prov["NAME_"])

        if prov_list:
            copy_result["PROVINCE_CODE_"] = "|".join(prov_code_list)
            copy_result["PROVINCE_NAME_"] = "|".join(prov_list)
        if city_list:
            copy_result["CITY_CODE_"] = "|".join(city_code_list)
            copy_result["CITY_NAME_"] = "|".join(city_list)
            # copy_result["AREA_CODE_"] = result[""]
            # copy_result["AREA_NAME_"] = result[""]
        if lat_list:
            copy_result["LAT_"] = "|".join(lat_list)
            copy_result["LNG_"] = "|".join(lng_list)

        if not copy_result["PRO_CODE_"]:
            copy_result["PRO_CODE_"] = hash(copy_result["PRO_NAME_"])

        end_arrow = arrow.get(copy_result["PRO_END_"])
        end_stamp = end_arrow.timestamp
        current_stamp = arrow.utcnow().timestamp
        if end_stamp > current_stamp:
            copy_result["PRO_STATUS_"] = "在售"
        else:
            copy_result["PRO_STATUS_"] = "停售"

        if not copy_result["OPT_MODE_"]:
            copy_result["OPT_MODE_"] = "开放式净值型"

        if not copy_result["SOURCE_RISK_LEVEL_"]:
            copy_result["SOURCE_RISK_LEVEL_"] = ["低等风险", "中高风险", "高风险", "二级", "中级"][random.randint(0, 4)]

        copy_result["INVEST_TYPE_"] = ["混合类", "债券类", "结构性投资类", "代客境外投资类", "银行存款类", "拆放同业及买入返售类", "同业存单类", "非标准化债权类", "另类投资类"][random.randint(0, 4)]
        invest_dict = {"混合类": "HHL", "债券类": "ZQL", "结构性投资类": "JGXTZL", "代客境外投资类": "DKJWTZL", "银行存款类": "YHCKL", "拆放同业及买入返售类": "CFTYJMRFSL", "同业存单类": "TYCDL", "非标准化债权类": "FBZHZQL", "另类投资类": "LLTZL"}
        copy_result["INVEST_TYPE_CODE_"] = invest_dict[copy_result["INVEST_TYPE_"]]
        copy_result["REDEEM_"] = random.randint(0, 1)

        if copy_result["INCREASE_"] == "北京":
            copy_result["INCREASE_"] = "10000"

        copy_result["HOT_"] = random.randint(1, 5)

        risk_dict = {"低": "R1", "中低": "R2", "中": "R3", "中高": "R4", "高": "R5"}
        copy_result["RISK_LEVEL_"] = ["低", "中低", "中", "中高", "高"][random.randint(0, 4)]
        copy_result["RISK_LEVEL_CODE_"] = risk_dict[copy_result["RISK_LEVEL_"]]

        self.p_client.table_name = "CHA_BRANCH_FINANCIAL_PRODUCT"
        return copy_result

    # 信用卡
    def CREDITCARD_FINASSIST(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        if result["ENTITY_CODE_"] == "ABCCARD":
            copy_result["ENTITY_NAME_"] = "农业银行信用卡"
        else:
            copy_result["ENTITY_NAME_"] = "中国银行信用卡"
        copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        # copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"]
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "NO"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", result["URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = copy_result["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = result[""]
        # copy_result["HOT_"] = result[""]
        copy_result["PRO_NAME_"] = result["CARD_NAME_"]
        # copy_result["TYPE_"] = result[""]
        # copy_result["USE_PERSON_"] = result[""]
        copy_result["IMAGES_"] = result["CARD_IMAGES_"]

        currency_dict = {"人民币单卡": "RMB", "多币卡": "DBZ"}
        copy_result["CURRENCY_TYPE_"] = ["人民币单卡", "多币卡"][random.randint(0, 1)]
        copy_result["CURRENCY_TYPE_CODE_"] = currency_dict[copy_result["CURRENCY_TYPE_"]]
        if result["BRAND_"]:
            brand_list = list()
            if "|" in result["BRAND_"]:
                if "UnionPay" in result["BRAND_"] and "MasterCard" in result["BRAND_"]:
                    copy_result["BRAND_"] = "银联+MasterCard"
                    copy_result["BRAND_CODE_"] = "YLK"
                elif "UnionPay" in result["BRAND_"] and "Visa" in result["BRAND_"]:
                    copy_result["BRAND_"] = "银联+VISA"
                    copy_result["BRAND_CODE_"] = "YL_VISA"
                # elif "MasterCard" in result["BRAND_"] and "Visa" in result["BRAND_"]:
                #     copy_result["BRAND_"] = ""
            else:
                if "UnionPay" in result["BRAND_"]:
                    copy_result["BRAND_"] = "银联"
                    copy_result["BRAND_CODE_"] = "YL"
                elif "MasterCard" in result["BRAND_"]:
                    copy_result["BRAND_"] = "MasterCard"
                    copy_result["BRAND_CODE_"] = "MC"
                elif "Visa" in result["BRAND_"]:
                    copy_result["BRAND_"] = "VISA"
                    copy_result["BRAND_CODE_"] = "VISA"

        # copy_result["BRAND_CODE_"] = result[""]
        # copy_result["LEVEL_"] = result[""]
        # copy_result["CONSUME_LIMIT_"] = result[""]
        # copy_result["POINTS_"] = result[""]
        # copy_result["VALID_DATE_POINTS_"] = result[""]
        copy_result["FEE_"] = result["FEE_"]
        # copy_result["FREE_POLICY_"] = result[""]
        # copy_result["PRE_BORROW_"] = result[""]
        # copy_result["GRACE_PERIODS_"] = result[""]
        # copy_result["VALID_CONSUME_"] = result[""]
        # copy_result["DAILY_INTEREST_"] = result[""]
        # copy_result["MIN_REPAY_"] = result[""]
        # copy_result["BILL_DATE_"] = result[""]
        # copy_result["STAGE_INFO_"] = result[""]
        # copy_result["SPECIAL_"] = result[""]
        # copy_result["VAS_"] = result[""]
        copy_result["RECOMMEND_"] = "NO"
        copy_result["GOOD_SALE_"] = "NO"
        copy_result["NEW_SALE_"] = "NO"
        # copy_result[""] = result[""]

        # fake data
        for pro in self.province_list:
            if pro["NAME_"][:2] in copy_result["PRO_NAME_"]:
                copy_result["PROVINCE_CODE_"] = pro["CODE_"]
                copy_result["PROVINCE_NAME_"] = pro["NAME_"]
                copy_result["LAT_"] = pro["LAT_"]
                copy_result["LNG_"] = pro["LNG_"]
                copy_result["UNIT_CODE_"] = "_".join([result["BANK_CODE_"], pro["CODE_"]])
                copy_result["UNIT_NAME_"] = "".join([result["BANK_NAME_"], pro["NAME_"], "分行"])
        for city in self.city_list:
            if city["NAME_"][:2] in copy_result["PRO_NAME_"]:
                copy_result["CITY_CODE_"] = city["CODE_"]
                copy_result["CITY_NAME_"] = city["NAME_"]
                copy_result["LAT_"] = city["LAT_"]
                copy_result["LNG_"] = city["LNG_"]
                copy_result["UNIT_CODE_"] = "_".join([result["BANK_CODE_"], city["CODE_"]])
                copy_result["UNIT_NAME_"] = "".join([result["BANK_NAME_"], city["NAME_"], "分行"])

        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"].replace("-", "")

        tags = re.findall(r"联名|ETC|私人银行", copy_result["PRO_NAME_"])
        if tags:
            copy_result["TAGS_"] = "|".join(tags)
        else:
            copy_result["TAGS_"] = "信用卡"

        copy_result["CONSUME_LIMIT_"] = str(random.randint(1, 10)) + "00000"

        copy_result["FREE_POLICY_"] = ["首年免年费", "消费5笔免年费", "终身免年费"][random.randint(0, 2)]

        copy_result["PRE_BORROW_"] = str(random.randint(1, 10)) + "00000"

        copy_result["GRACE_PERIODS_"] = str(random.randint(1, 50))

        copy_result["DAILY_INTEREST_"] = str(random.random())[:4]

        copy_result["BILL_DATE_"] = str(random.randint(1, 28))
        # TODO this and pro_name_
        copy_result["SPECIAL_"] = copy_result["TAGS_"]

        self.p_client.table_name = "CHA_BRANCH_CREDITCARDARD"

        copy_result["HOT_"] = random.randint(1, 5)
        type_dict = {"标准卡": "BZK", "购物卡": "GWK", "车主卡": "CZK", "卡通卡": "KTK", "商旅卡": "SLK", "游戏卡": "YXK",
                     "多倍积分卡": "DBJFK", "主题卡": "ZTK", "影视卡": "YSK"}
        copy_result["TYPE_"] = ["标准卡", "购物卡", "车主卡", "卡通卡", "商旅卡", "游戏卡", "多倍积分卡", "主题卡", "影视卡"][random.randint(0, 8)]
        copy_result["TYPE_CODE_"] = type_dict[copy_result["TYPE_"]]

        level_dict = {"普卡": "PK", "金卡": "JK", "白金卡": "BJK", "钻石卡": "ZSK", "钛金卡": "TJK", "无限卡": "WXK", "小白金": "XBK",
                      "银卡": "YK", "世界卡": "SJK", "铂金卡": "BOJK", "贵宾卡": "GBK", "Signature卡": "SK"}
        copy_result["LEVEL_"] = \
            ["普卡", "金卡", "白金卡", "钻石卡", "钛金卡", "无限卡", "小白金", "银卡", "世界卡", "铂金卡", "贵宾卡", "Signature卡"][
                random.randint(0, 11)]
        copy_result["LEVEL_CODE_"] = level_dict[copy_result["LEVEL_"]]

        return copy_result

    # 信用卡活动 》》 营销活动
    def CREDITCARDACT_FINASSIST(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = "卡讯网优惠活动"
        copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"].replace("-", "")
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "NO"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", result["URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = copy_result["ENTITY_NAME_"]
        copy_result["SOURCE_TYPE_"] = "信用卡活动"
        copy_result["SOURCE_TYPE_"] = "XYK"
        # copy_result["HOT_"] = result[""]

        copy_result["IMPORTANCE_"] = "NO"
        # copy_result["ACT_NAME_"] = copy_result["TITLE_"]
        # copy_result["IMAGES_"] = data[""]
        copy_result["TARGET_"] = result["TARGET_"]
        # copy_result["BRIEF_"] = data[""]
        copy_result["DETAILS_"] = result["DETAILS_"]
        copy_result["RULE_"] = result["RULE_"]
        copy_result["START_TIME_"] = result["START_TIME_"]
        copy_result["END_TIME_"] = result["END_TIME_"]
        copy_result["ACT_TYPE1_"] = "信用卡活动"
        copy_result["ACT_TYPE1_CODE_"] = "XYKHD"
        # copy_result["ACT_TYPE2_"] = data[""]
        # copy_result["ACT_TYPE3_"] = data[""]
        copy_result["PUBLISH_TIME_"] = result["PERIOD_CODE_"]
        # copy_result["READS_"] = data[""]
        # copy_result["LIKES_"] = data[""]
        # copy_result["COMMENTS_"] = data[""]
        # copy_result["JOINS_"] = data[""]
        # copy_result["RELAYS_"] = data[""]
        # copy_result["SOURCE_ID_"] = data[""]
        # copy_result["HTML_"] = data[""]
        # copy_result["SOURCE_OWN_NAME_"] = data[""]
        # copy_result["SOURCE_OWN_ID_"] = data[""]
        # copy_result[""] = result[""]

        self.p_client.table_name = "CHA_BRANCH_MARKET_ACT"
        # fake data
        copy_result["HOT_"] = random.randint(1, 5)
        return copy_result

    # 保险
    def INSURANCE(self, result):
        copy_result = dict()
        copy_result["ID_"] = result["ID_"]
        copy_result["ENTITY_CODE_"] = result["ENTITY_CODE_"]
        copy_result["ENTITY_NAME_"] = result["ENTITY_NAME_"]
        copy_result["URL_"] = result["URL_"]
        # copy_result["PROVINCE_CODE_"] = result[""]
        # copy_result["PROVINCE_NAME_"] = result[""]
        # copy_result["CITY_CODE_"] = result[""]
        # copy_result["CITY_NAME_"] = result[""]
        # copy_result["AREA_CODE_"] = result[""]
        # copy_result["AREA_NAME_"] = result[""]
        # copy_result["LAT_"] = result[""]
        # copy_result["LNG_"] = result[""]
        # copy_result["APP_VERSION_"] = "BRANCH"
        copy_result["BANK_CODE_"] = result["BANK_CODE_"]
        copy_result["BANK_NAME_"] = result["BANK_NAME_"]
        copy_result["UNIT_CODE_"] = result["UNIT_CODE_"]
        # copy_result["UNIT_NAME_"] = result[""]
        copy_result["PERIOD_CODE_"] = result["PERIOD_CODE_"].replace("-", "")
        # copy_result["REMARK_"] = result[""]
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        copy_result["CREATE_TIME_"] = create_time
        copy_result["SPIDER_TIME_"] = result["CREATE_TIME_"]
        # copy_result["MODIFIED_TIME_"] = result[""]
        copy_result["CREATE_BY_ID_"] = "P0131857"
        copy_result["CREATE_BY_NAME_"] = "钟楷文"
        # copy_result["MODIFIED_BY_ID_"] = result[""]
        # copy_result["MODIFIED_BY_NAME_"] = result[""]
        copy_result["M_STATUS_"] = "NO"
        copy_result["DELETE_STATUS_"] = "NO"
        copy_result["DATA_STATUS_"] = "uncheck"
        copy_result["PUBLISH_STATUS_"] = "NO"
        # copy_result["TAGS_"] = result[""]
        source = re.findall(r"(https?://.*?)/", result["URL_"])
        copy_result["SOURCE_"] = source[0]
        copy_result["SOURCE_NAME_"] = copy_result["ENTITY_NAME_"]
        # copy_result["SOURCE_TYPE_"] = "CREDITCARDACT_FINASSIST"
        # copy_result["HOT_"] = result[""]

        copy_result["PRO_NAME"] = result["PRODUCT_NAME_"]
        copy_result["COM_NAME_"] = result["COM_NAME_"]
        copy_result["ENSURE_PRICE_"] = result["ENSURE_PRICE_"]
        # copy_result["ENSURE_FEE_"] = result[""]
        # copy_result["SPECAIL_"] = result[""]
        # copy_result["BRIEF_"] = result[""]
        copy_result["AGE_"] = result["AGE_"]
        # copy_result["ENSURE_DATE_"] = result[""]
        copy_result["BUY_LIMIT_"] = result["BUY_LIMIT_"]
        # copy_result["ENSURE_MODE_"] = result[""]
        # copy_result["ENSURE_MODE_CODE_"] = result[""]
        # copy_result["SUIT_"] = result[""]
        # copy_result["SUIT_CODE_"] = result[""]
        copy_result["PRO_DETAIL_"] = result["PRODUCT_DETAIL_"]
        # copy_result["ENSURE_CONTENT_"] = result[""]
        # copy_result["NOTICE_"] = result[""]
        # copy_result["IMAGES_"] = result[""]
        # copy_result["ENSURE_SOURCE_TYPE_"] = result[""]
        for each in ["寿险", "年金", "意外", "个人财", "企业财", "旅游", "健康", "理财"]:
            if each in copy_result["PRO_NAME"]:
                if each == "寿险":
                    copy_result["ENSURE_TYPE_"] = each
                else:
                    copy_result["ENSURE_TYPE_"] = each + "险"
                break
            else:
                copy_result["ENSURE_TYPE_"] = result["TYPE_"]
        # copy_result["HTML_"] = result[""]
        copy_result["RECOMMEND_"] = "NO"
        copy_result["GOOD_SALE_"] = "NO"
        copy_result["NEW_SALE_"] = "NO"

        self.p_client.table_name = "CHA_BRANCH_INSURANCE"

        # fake data
        copy_result["PROVINCE_CODE_"] = "0000"
        copy_result["PROVINCE_NAME_"] = "中国(省)"
        copy_result["CITY_CODE_"] = "000000"
        copy_result["CITY_NAME_"] = "中国(市)"
        copy_result["AREA_CODE_"] = "000001"
        copy_result["AREA_NAME_"] = "中国(区县)"
        copy_result["LAT_"] = "37.5503394745908"
        copy_result["LNG_"] = "104.11412925347894"

        copy_result["ENSURE_PRICE_"] = str(random.randint(1, 10)) + "00000"
        copy_result["ENSURE_FEE_"] = str(random.randint(1, 10)) + "00"
        special = re.findall(r"保?至?终身|分红型|万能型|至\d+周岁|尊享版", copy_result["PRO_NAME"])
        if special:
            copy_result["SPECAIL_"] = "|".join(special)
        copy_result["BRIEF_"] = "简介:" + copy_result["PRO_NAME"]
        copy_result["AGE_"] = str([20, 30, 40, 50][random.randint(0, 3)])
        copy_result["ENSURE_DATE_"] = re.findall(r"至\d+周岁", copy_result["PROVINCE_NAME_"])
        copy_result["BUY_LIMIT_"] = 1
        # copy_result["ENSURE_MODE_"] =
        copy_result["SUIT_"] = copy_result["AGE_"] + "以下无大病者"
        copy_result["HOT_"] = random.randint(1, 5)
        return copy_result

    def run(self):
        # # 复制数据到新的表
        # self.p_client.copy_table_phoenix(connection=connection, source_table="CommonBidding", new_table="CommonBidding_test")

        # # 删除表
        # self.p_client.drop_table_phoenix(connection=connection, table_name="ORGANIZE_FINASSIST_TEST")

        # # 创建表
        # column_arrary_list = [("C", "BANK_NAME_"), ("C", "BANK_CODE_"), ("C", "NAME_"), ("C", "CODE_"),
        #                       ("C", "ENTITY_NAME_"), ("C", "ENTITY_CODE_"), ("C", "AREA_CODE_"), ("C", "UNIT_CODE_"),
        #                       ("C", "ADDR_"), ("C", "PROVINCE_NAME_"), ("C", "PROVINCE_CODE_"), ("C", "CITY_"),
        #                       ("C", "CITY_CODE_"), ("C", "DISTRICT_NAME_"), ("C", "DISTRICT_CODE_"), ("C", "LAT_"),
        #                       ("C", "LNG_"), ("C", "CREATE_TIME_"), ("C", "DEALTIME_"), ("C", "URL_"), ("C", "TEL_"),
        #                       ("C", "BUSINESS_HOURS_"), ("C", "STATUS_"), ("C", "IMPORTANCE_")]
        #
        # self.p_client.create_table_phoenix(connection=connection, column_arrary_list=column_arrary_list,
        #                             table_name="MARKETING_ACT")

        # # 查询
        # # 返回列表
        # result = self.p_client.search_all_from_phoenix(connection=connection, iter_status=False, dict_status=True)
        # print(result)

        # 返回生成器对象
        # result_generator = self.p_client.search_all_from_phoenix(connection=connection, dict_status=True,
        #                                                   where_condition="ENTITY_STATUS_ = 'DRAFT'")
        result_generator = self.p_client.search_all_from_phoenix(connection=self.connection, dict_status=True,
                                                                 limit_num=101,
                                                                 where_condition="ENTITY_CODE_ is not null")
        from scripts import GenericScript
        self.province_list, self.city_list, self.area_list, dir_area_list, bank_list = GenericScript.data_from_mysql()

        while True:
            try:
                result = result_generator.__next__()
                if self.script_name[:3] != "CHA":
                    if self.script_name == "WEIBOINFO":
                        e_func = "".join(["self.", self.script_name, "(result)"])
                        copy_result = eval(e_func)
                        self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=copy_result)
                        comment_result = self.WEIBO_COMMENT(result)
                        if comment_result:
                            for comment in comment_result:
                                self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=comment)
                    else:
                        e_func = "".join(["self.", self.script_name, "(result)"])
                        copy_result = eval(e_func)
                        self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=copy_result)
                else:
                    if self.script_name == "CHA_BRANCH_MARKET_ACT":
                        e_func = "".join(["self.", "MARKETING_ACT", "(result)"])
                        copy_result = eval(e_func)
                        self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=copy_result)
                    else:
                        pass
            except StopIteration:
                break
            # except Exception as e:
            #     print(e)
            #     continue

        # to csv
        # from tools import to_csv
        # while True:
        #     try:
        #         to_csv.to_csv(result_generator.__next__())
        #     except StopIteration:
        #         break

        # self.connection.close()


if __name__ == '__main__':
    # table_list = ["NEWS_FINASSIST", "GWNEWS_FINASSIST", "WECHAT", "WEIBOBASICINFO", "WEIBOINFO",
    #               "CHA_BRANCH_MARKET_ACT", "FINPRODUCT_FINASSIST", "CREDITCARD_FINASSIST", "CREDITCARDACT_FINASSIST",
    #               "INSURANCE"]  CHA_BRANCH_FINANCIAL_PRODUCT
    table_list = ["FINPRODUCT_FINASSIST"]
    script = Script(table_name=table_list[0])
    for table in table_list:
        script.script_name = table
        script.p_client.table_name = table
        script.p_client.position_dict = dict()
        # script = Script(table_name="INSURANCE")
        script.run()
    script.connection.close()
