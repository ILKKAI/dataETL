# -*- coding: utf-8 -*-
import base64
import os
import re
import sys


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-15])

from branch_scripts2 import GenericScript
from tools.req_for_ai import req_for_ts, req_for_censor, req_for_weibo_hot, req_for_comment
from tools.req_for_api import req_for_serial_number, req_for_something
from __config import TABLE_NAME


class WeiboScript(GenericScript):

    def __init__(self, table_name, collection_name, param):
        verify_field = {"URL_": "CONTENT_URL_"}
        self.re_verify_field = verify_field
        super(WeiboScript, self).__init__(table_name, collection_name, param, verify_field=verify_field)
        self.p_client.position_dict = dict()
        __weibo_list = self.p_client.search_all_from_phoenix(connection=self.connection, table_name="CHA_BRANCH_WEIBO_BASIC", dict_status=True, iter_status=False)

        self.weibo_list = [weibo for weibo in __weibo_list]

    def generic_shuffle(self, data):
        re_data = list()
        # CHA_BRANCH_WEIBO_INFO
        info_data = dict()
        serial_number = req_for_serial_number(code="WEIBO_INFO")
        info_data["ID_"] = serial_number
        print(serial_number)

        info_data["ENTITY_CODE_"] = data["BANK_CODE_"]

        info_data["URL_"] = data["CONTENT_URL_"]

        info_data["PERIOD_CODE_"] = data["PUBLISH_TIME_"].replace("-", "")
        # 数据来源 URL
        source = re.findall(r"(https?://.*?)/", data["CONTENT_URL_"])
        info_data["SOURCE_"] = source[0]
        # 数据来源 网站名称
        info_data["SOURCE_NAME_"] = data["ENTITY_NAME_"].split("-")[0]

        info_data["SOURCE_TYPE_"] = "WEIBO"

        info_data["LIKES_"] = data["PRAISES_"]
        if not info_data["LIKES_"]:
            info_data["LIKES_"] = 0
        info_data["COMMENTS_"] = data["REPLIES_"]
        if not info_data["COMMENTS_"]:
            info_data["COMMENTS_"] = 0
        info_data["RELAYS_"] = data["RELAYS_"]
        if not info_data["RELAYS_"]:
            info_data["RELAYS_"] = 0
        info_data["IMPORTANCE_"] = "N"
        info_data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"]
        info_data["CONTENT_"] = data["CONTENT_"]
        if data.get("CONTENT_IMAGES_") and len(data["CONTENT_IMAGES_"]) > 0:
            for each_image in data["CONTENT_IMAGES_"]:
                response = req_for_something(url=each_image)
                if response:
                    t = base64.b64encode(response.content)
                    info_data[f"IMAGE_{data['CONTENT_IMAGES_'].index(each_image)+1}"] = t.decode("utf-8")
                    response.close()

        # 补录
        # info_data["TYPE_"] = data[""]
        # info_data["TYPE_CODE_"] = data[""]
        info_data["PUBLISH_STATUS_"] = "N"
        if "OWN_" in data:
            if data["OWN_"] == "转载":
                info_data["OWN_"] = "N"
            else:
                info_data["OWN_"] = "Y"

        for each in self.weibo_list:
            if each["WEIBO_NAME_"] == data["ENTITY_NAME_"]:
                info_data["WEIBO_CODE_"] = each["WEIBO_CODE_"]
                info_data["WEIBO_NAME_"] = each["WEIBO_NAME_"]
                break
        # 模型
        # 摘要
        try:
            brief = req_for_ts(info_data["CONTENT_"])
            if brief:
                info_data["BRIEF_"] = brief["summary"]
        except Exception as e:
            self.logger.info(f"调用模型req_for_ts失败，原因为{e}")
            info_data["BRIEF_"] = ""
        # 是否敏感
        try:
            censor = req_for_censor(info_data["CONTENT_"])
            if censor:
                if censor["censor"] == "N":
                    info_data["SENSITIVE_"] = "N"
                else:
                    info_data["SENSITIVE_"] = "Y"
                    info_data["SENSITIVE_WORD_"] = censor["words"]
        except Exception as e:
            self.logger.info(f"调用模型censor失败，错误为{e}")
            info_data["SENSITIVE_"] = "N"

        info_data["VERSION_"] = "0"
        info_data = super(WeiboScript, self).generic_shuffle(data=data, re_data=info_data, field="ENTITY_NAME_")
        # 清洗浦发银行BANK_NAME_和BANK_CODE_
        if info_data["ENTITY_NAME_"] == "上海浦东发展银行微博":
            info_data["BANK_NAME_"] = "浦发银行"
            info_data["BANK_CODE_"] = "SPDB"
        if info_data["ENTITY_NAME_"] == "南海农商银行微博":
            info_data["BANK_NAME_"] = "广东南海农村商业银行股份有限公司"
            info_data["BANK_CODE_"] = "NRC"
        if info_data["ENTITY_NAME_"] == "顺德农商银行微博":
            info_data["BANK_NAME_"] = "广东顺德农村商业银行股份有限公司"
            info_data["BANK_CODE_"] = "sdebank"

        comment = data["INFO_COMMENTS_"]
        verifieds = 0
        for c in comment:
            if c.get("VERIFIED_", ""):
                verifieds += 1

        # 微博热度
        try:
            hot = req_for_weibo_hot(publish_time=info_data["PUBLISH_TIME_"], relays=info_data["RELAYS_"],
                                    replies=len(comment), praises=info_data["LIKES_"], verifieds=verifieds)
            if hot:
                info_data["HOT_"] = hot["level"]
        except Exception as e:
            self.logger.info(f"调用模型weibo_hot失败，错误为{e}")

        re_data.append({"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_WEIBO_INFO"), "DATA_": info_data})
        if len(comment) > 0:
            comment_count = 0
            for each in comment:
                # CHA_BRANCH_WEIBO_COMMENT
                # 每次需要初始化comment_data不然导致数据重复
                comment_data = dict()
                # HBase row_key
                serial_number = req_for_serial_number(code="WEIBO_COMMENT")
                comment_data["ID_"] = serial_number
                comment_data["INFO_ID_"] = info_data["ID_"]
                comment_data["COMMENT_"] = each["COMMENT_"]
                comment_data["REPLIER_TIME_"] = each["REPLIER_TIME_"]
                comment_data["REPLIER_HEAD_"] = each["REPLIER_HEAD_"]
                comment_data["REPLIER_PRAISES_"] = each["REPLIER_PRAISES_"]
                comment_data["REPLIER_"] = each["REPLIER_"]
                comment_data["REPLIER_REPLIES_"] = each["REPLIER_REPLIES_"]
        # 情感分析

                if each.get("COMMENT_") and len(each["COMMENT_"]) > 0:
                    try:
                        sentiment = req_for_comment(each["COMMENT_"])
                        if sentiment:
                            if sentiment["sentiment"] == "中性":
                                comment_data["EMOTION_"] = "NORMAL"
                            if sentiment["sentiment"] == "积极":
                                comment_data["EMOTION_"] = "POSITIVE"
                            if sentiment["sentiment"] == "敏感":
                                comment_data["EMOTION_"] = "NAGETIVE"
                        else:
                            comment_data["EMOTION_"] = "NORMAL"
                    except Exception as e:
                        self.logger.info(f"调用模型req_for_comment失败，错误为{e}")
                        comment_data["EMOTION_"] = "NORMAL"

        # 是否敏感
                    try:
                        censor = req_for_censor(each["COMMENT_"])
                        if censor:
                            if censor["censor"] == "N":
                                comment_data["SENSITIVE_"] = "N"
                            else:
                                comment_data["SENSITIVE_"] = "Y"
                                comment_data["SENSITIVE_WORD_"] = censor["words"]
                        else:
                            comment_data["SENSITIVE_"] = "N"
                    except Exception as e:
                        self.logger.info(f"调用模型req_for_comment失败，错误为{e}")
                        comment_data["SENSITIVE_"] = "N"

                comment_data["VERSION_"] = "0"
                comment_data["CREATE_BY_ID_"] = "P0131857"
                comment_data["CREATE_BY_NAME_"] = "钟楷文"
                re_data.append({"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_WEIBO_COMMENT"), "DATA_": comment_data})
                comment_count += 1
            # 打相关评论日志方便调试
            self.logger.info(f'清洗的URL为{info_data["URL_"]}')
            self.logger.info(f'清洗的评论数为{info_data["COMMENTS_"]}')
            self.logger.info(f'插入到comment表的数量为{comment_count}')
        # print(re_data)
        return re_data


if __name__ == '__main__':
    # try:
    #     param = sys.argv[1]
    # except Exception:
    #     param = '{}'
    param = "{'limitNumber':'1000'}"
    script = WeiboScript(table_name=TABLE_NAME("CHA_BRANCH_WEIBO_INFO"), collection_name="WEIBOINFO", param=param)
    script.main()
    script.close_client()
