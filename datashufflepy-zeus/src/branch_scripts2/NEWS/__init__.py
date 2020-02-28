# -*- coding: utf-8 -*-
"""新闻通用清洗脚本"""
import base64
import os
import re
import sys

from bs4 import BeautifulSoup

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-15])

from tools.req_for_api import req_for_something
from tools.web_api_of_baidu import get_lat_lng, get_area
from branch_scripts2 import GenericScript
from tools.req_for_ai import req_for_ts, req_for_senti, req_for_censor, req_for_news_hot, req_for_textLoc, \
    req_for_credit_relative
from __config import TABLE_NAME


class BranchNews(GenericScript):

    def __init__(self, table_name, collection_name, param):
        super(BranchNews, self).__init__(table_name=table_name, collection_name=collection_name,
                                         param=param, verify_field={"URL_":"URL_"})
        self.script_path = self.m_client.mongo_collection

    def generic_shuffle(self, data, field="CONTENT_"):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        # different shuffle rule
        re_data = dict()

        if "PUBLISH_TIME_" not in data:
            return None
        # 时间维度
        if re.findall(r"\d{4}-\d{1,2}-\d{1,2}", data["PUBLISH_TIME_"]):
            pass
        elif re.findall(r"\d{4}年\d{1,2}月\d{1,2}日", data["PUBLISH_TIME_"]):
            data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"].replace("年", "-")
            data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"].replace("月", "-")
            data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"].replace("日", "")

        else:
            if ("年" in data["PUBLISH_TIME_"]) and ("月" in data["PUBLISH_TIME_"]) and ("二" in data["PUBLISH_TIME_"]):
                format_list = list()
                for i in data["PUBLISH_TIME_"][:10]:
                    format_list.append(self.number_dict[i])
                    data["PUBLISH_TIME_"] = "".join(format_list)

            # 暂无其他情形
            # elif
            else:
                find_time = re.findall(r"\|(\w{4}[-年]\w{1,2}[-月]\w{1,2})日?\W?\|", data["CONTENT_"])
                if find_time:
                    if "二" in find_time[0]:
                        format_list = list()
                        for i in find_time[0]:
                            format_list.append(self.number_dict[i])
                            data["PUBLISH_TIME_"] = "".join(format_list)
                    else:
                        data["PUBLISH_TIME_"] = find_time[0]
                        data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"].replace("年", "-")
                        data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"].replace("月", "-")
                        data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"].replace("日", "")
                else:
                    data["PUBLISH_TIME_"] = ""

        if data["PUBLISH_TIME_"]:
            shuffle_list = data["PUBLISH_TIME_"].split("-")
            shuffle_list[0] = shuffle_list[0][:4]
            if len(shuffle_list[1]) == 2:
                pass
            elif len(shuffle_list[1]) == 1:
                shuffle_list[1] = "0" + shuffle_list[1]
            elif len(shuffle_list[1]) > 2:
                shuffle_list[1] = shuffle_list[1][:2]

            if len(shuffle_list[2]) == 2:
                pass
            elif len(shuffle_list[2]) == 1:
                shuffle_list[2] = "0" + shuffle_list[2]
            elif len(shuffle_list[2]) > 2:
                shuffle_list[2] = shuffle_list[2][:2]

            data["PUBLISH_TIME_"] = "-".join(shuffle_list)

        re_data["PERIOD_CODE_"] = data["PUBLISH_TIME_"].replace("-", "")

        # re_data["REMARK_"] = ""

        # 标签
        if "TAGS_" in data:
            re_data["TAGS_"] = ""

        # 数据来源 URL
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        re_data["SOURCE_"] = source[0]
        # 数据来源 网站名称
        re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"].split("-")[0]

        # 数据来源编码
        s_index = data["ENTITY_CODE_"].rfind("_")
        re_data["SOURCE_CODE_"] = data["ENTITY_CODE_"][:s_index]

        # 资讯来源分类
        re_data["SOURCE_TYPE_"] = data["ENTITY_CODE_"][3:7]

        re_data["PUBLISH_TIME_"] = data["PUBLISH_TIME_"]
        re_data["TITLE_"] = data["TITLE_"]

        # 作者
        if "AUTHOR_" in data:
            if "编辑" in data["AUTHOR_"]:
                re_data["AUTHOR_"] = re.findall(r"编辑[:：](\w+)", data["AUTHOR_"])[0]
            else:
                re_data["AUTHOR_"] = data["AUTHOR_"]

        re_data["IMPORTANCE_"] = "N"
        # 阅读数
        if "READ_" in data:
            re_data["READS_"] = data["READ_"]
        else:
            re_data["READS_"] = 0
        # 点赞数
        if "LIKES_" in data:
            re_data["LIKES_"] = data["LIKES_"]
        else:
            re_data["LIKES_"] = 0
        # 评论数
        if "COMMENTS_" in data:
            re_data["COMMENTS_"] = data["COMMENTS_"]
        elif "COMMENT_" in data:
            re_data["COMMENTS_"] = data["COMMENT_"]
        else:
            re_data["COMMENTS_"] = 0
        # 参与数
        if "JOINS_" in data:
            re_data["JOINS_"] = data["JOINS_"]
        elif "JOIN_" in data:
            re_data["JOINS_"] = data["JOIN_"]
        else:
            re_data["JOINS_"] = 0

        # 内容
        re_data["CONTENT_"] = re.sub(r"(var.*?;\|)(?![a-zA-Z])", "", data["CONTENT_"])

        # HTML 标签
        re_data['CONTENT_HTML_'] = data["HTML_"]
        data["CONTENT_HTML_"] = data["HTML_"]
        re_data["CONTENT_HTML_"] = re.sub(r"href=\".*?\"", "href=\"javaScript:void(0);\"", re_data["CONTENT_HTML_"])

        if '28857' in re_data['CONTENT_HTML_'] or '您的IP' in re_data['CONTENT_HTML_']:
            try:
                soup = BeautifulSoup(re_data['CONTENT_HTML_'])
                soup.find('div', attrs={'class': 'online-desc-con'}).decompose()
                soup.find_all('script')[0].decompose()
                re_data['CONTENT_HTML_'] = soup.prettify()
            except Exception as e:
                self.logger.exception(f'IP检测内容清除出错')

        # TODO del data["HTML_] is wrong
        del data["HTML_"]
        re_data["CONTENT_"] = re_data["CONTENT_"].replace("|", "")
        re_data["TITLE_"] = re_data["TITLE_"].replace("|", "")
        # 是否营销活动
        re_data["ACT_"] = "N"

        # 版本
        re_data["VERSION_"] = "0"

        if "IMAGE_" in data:
            try:
                response = req_for_something(url=data["IMAGE_"])
                if response:
                    t = base64.b64encode(response.content)
                    data["IMAGE_"] = t.decode("utf-8")
                    response.close()
            except Exception:
                pass

        # 调用模型
        # 摘要
        try:
            brief = req_for_ts(re_data["CONTENT_"][0:1000])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_ts 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if brief:
                re_data["BRIEF_"] = brief["summary"]
            else:
                re_data["BRIEF_"] = '暂无摘要'
        # 情感分析
        try:
            sentiment = req_for_senti(re_data["TITLE_"])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_senti 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if sentiment:
                if sentiment["sentiment"] == "中性":
                    re_data["EMOTION_"] = "NORMAL"
                if sentiment["sentiment"] == "正面":
                    re_data["EMOTION_"] = "POSITIVE"
                if sentiment["sentiment"] == "敏感":
                    re_data["EMOTION_"] = "NAGETIVE"

        # 是否敏感
        try:
            censor = req_for_censor(re_data["CONTENT_"])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_censor 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if censor:
                if censor["censor"] == "N":
                    re_data["SENSITIVE_"] = "N"
                else:
                    re_data["SENSITIVE_"] = "Y"
                    re_data["SENSITIVE_WORD_"] = censor["words"]
        # 热度
        try:
            hot = req_for_news_hot(title=re_data["TITLE_"], content=re_data["CONTENT_"][0:1000])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_news_hot 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if hot:
                re_data["HOT_"] = hot["level"]

        # 地址分析
        try:
            res = req_for_textLoc(text=re_data["CONTENT_"])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_textLoc 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if "error" not in res:
                if res["tagsId"] == "None" or res["tagsId"] is None:
                    pass
                else:
                    re_data["TAGS_"] = res["tagsId"]
                if res["flag"] == 1:
                    address = res["full"]
                else:
                    address = res["addr"]
                try:
                    lat_result = get_lat_lng(address=address)
                    re_data["LAT_"] = lat_result["result"]["location"]["lat"]
                    re_data["LNG_"] = lat_result["result"]["location"]["lng"]
                except KeyError:
                    re_data["LAT_"] = None
                    re_data["LNG_"] = None
                except Exception as e:
                    self.logger.info(f"获取经纬度失败, ERROR: {e}")
                    re_data["LAT_"] = None
                    re_data["LNG_"] = None
                if re_data["LAT_"]:
                    try:
                        area_result = get_area(",".join([str(re_data["LAT_"]), str(re_data["LNG_"])]))
                    except Exception as e:
                        self.logger.info(f"获取地址失败, ERROR: {e}")
                    else:
                        try:
                            re_data["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                        except KeyError:
                            re_data["AREA_NAME_"] = ""
                        try:
                            re_data["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                        except KeyError:
                            re_data["AREA_CODE_"] = ""
                        else:
                            re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
                            re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
                            for city in self.city_list:
                                if city["CODE_"] == re_data["CITY_CODE_"]:
                                    re_data["CITY_NAME_"] = city["NAME_"]
                                    break
                            for prov in self.province_list:
                                if prov["CODE_"] == re_data["PROVINCE_CODE_"]:
                                    re_data["PROVINCE_NAME_"] = prov["NAME_"]
                                    break

        # 信用卡关联性
        try:
            res = req_for_credit_relative(text=re_data["CONTENT_"])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_credit_relative 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if res["creditrelative"]:
                re_data["MODULE_TYPE_"] = "CREDITCARD"

        # 银行名称、编码
        if "BANK_NAME_" in data:
            re_data["BANK_NAME_"] = data["BANK_NAME_"]
        if "BANK_CODE_" in data:
            re_data["BANK_CODE_"] = data["BANK_CODE_"]

        re_data = super(BranchNews, self).generic_shuffle(data=data, re_data=re_data, field="CONTENT_")

        # 财资直接发布
        re_data['DATA_STATUS_'] = 'CHECK'
        # 是否发布
        if not re_data.get("PUBLISH_TIME_"):
            re_data["PUBLISH_STATUS_"] = "N"
        else:
            re_data["PUBLISH_STATUS_"] = "Y"

        return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]


if __name__ == '__main__':
    try:
        # param = sys.argv[1]
        param = "{'entityType':'NEWS','limitNumber':10000,'entityCode':['ZX_GWDT_HEBYH_NHXW']}"
    except Exception:
        param = {}

    param_dict = eval(param)
    if "entityCode" in param_dict:
        if isinstance(param_dict["entityCode"], str):
            c = param_dict["entityCode"].split("_")
            if c[1] == "CJXW":
                coll = "_".join([c[0], c[1], c[2]])
            else:
                coll = "_".join([c[0], c[1]])
            script = BranchNews(table_name=TABLE_NAME("CHA_BRANCH_NEWS"), collection_name=coll, param=param)
            script.main()
            script.close_client()

        elif isinstance(param_dict["entityCode"], list):
            param_each = param_dict
            for each in param_dict["entityCode"]:
                c = each.split("_")
                if c[1] == "CJXW":
                    coll = "_".join([c[0], c[1], c[2]])
                else:
                    coll = "_".join([c[0], c[1]])
                param_each = param_dict
                param_each["entityCode"] = each
                script = BranchNews(table_name=TABLE_NAME("CHA_BRANCH_NEWS"), collection_name=coll, param=str(param_each))
                script.main()
            script.close_client()
    else:
        for coll in ["ZX_GWDT", "ZX_ZCGG", "ZX_HYBG", "ZX_CJXW_GJJRJG", "ZX_CJXW_ZYCJ", "ZX_CJXW_HY"]:
            # script = BranchNews(table_name="CHA_BRANCH_NEWS", collection_name=coll, param=sys.argv[1])
            script = BranchNews(table_name=TABLE_NAME("CHA_BRANCH_NEWS"), collection_name=coll, param=param)
            script.main()
        script.close_client()
