# -*- coding: utf-8 -*-

# 重庆市公共资源交易网网站 500000CQGGZY

# 11999  无 WIN_CANDIDATE_ 字段 已完成
# 没有 NOTICE_TIME_ 父级界面有

import re
import time
import pymongo

from database._mongodb import MongoClient


def data_shuffle(data):
    fina_result = ""
    if "中标" in data["TITLE_"] or "结果" in data["TITLE_"]:
        if ("分包号：2" in data["CONTENT_"] or "标段二" in data["CONTENT_"] or "二标段" in data["CONTENT_"]
                or "包二" in data["CONTENT_"] or "3标段" in data["CONTENT_"] or "分包号：3" in data["CONTENT_"]
                or "2标包" in data["CONTENT_"] or "第二包" in data["CONTENT_"] or "第三包" in data["CONTENT_"]
                or "分包2" in data["CONTENT_"]):
            fina_result = ""

        elif "入围" in data["TITLE_"]:
            fina_result = ""

        elif "废标原因" in data["CONTENT_"] and ("中标候选人" not in data["CONTENT_"]):
            fina_result = ""

        else:
            result = re.findall(
                r"第一中[标选]\|?（?选?）?[候侯]选人\|?单?位?名?称?为?[:：]?\|?(\w*[(（]?\w*[)）]?\w*、?\w*[(（]?\w*[)）]?\w*)",
                data["CONTENT_"])
            if result:
                if (len(result) == 1) and (len(result[0]) > 2):
                    fina_result = result[0]

                else:
                    i = 0
                    for r in result:
                        if ("公司" in r or "经营部" in r or "勘测院" in r or "事务所" in r or "大学" in r or "车队" in r
                                or "广播电台" in r or "电视台" in r or "报社广告中心" in r or "报社" in r or "代表处" in r
                                or "规划院" in r or "出版社" in r or "研究院" in r or "整治中心" in r or "医院" in r
                                or "园艺场" in r or "开发局" in r or "设计院" in r):
                            fina_result = r
                            i += 1

                    if i > 1:
                        fina_result = ""

                    if i == 0:
                        re_result = re.findall(r"第一[候侯]选人为?[:：]?[|]?(\w*[(（]?\w*[)）]?\w*、?\|?\w*[(（]?\w*[)）]?\w*)",
                                               data["CONTENT_"])
                        if re_result:
                            if "第二候选人" in re_result[0]:
                                fina_result = re_result[0].replace("|第二候选人", "")
                            else:
                                fina_result = re_result[0].replace("|", "")

                        else:
                            re_result = re.findall(r"(\w*[(（]?\w*[)）]?\w*)[,，]?为?第一中?标?[候侯]选人", data["CONTENT_"])
                            if re_result:
                                if len(re_result[0]) > 2:
                                    fina_result = re_result[0]
                                else:
                                    result = []
                            else:
                                result = []

            if not result:
                result = re.findall(r"第一中标[供人]应?商?为?[:：]?\|?[\"“]?(\w*[(（]?\w*[)）]?\w*)[\"”]?", data["CONTENT_"])
                if result:
                    fina_result = result[0]

            if not result:
                result = re.findall(r"第一候选成交供应商\|(\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])
                if result:
                    fina_result = result[0]

            if not result:
                result = re.findall(r"第一成交候选[供人]应?商?\w*[:：]?\|?(\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])
                if result:
                    fina_result = result[0].replace("评标得分", "")
                    fina_result = fina_result.replace("报价金额", "")
                    if not result[0]:
                        result = []

            if not result:
                result = re.findall(r"第一拟?中标侯?标?人为?[:：]?[|]?(\w*[(（]?\w*[)）]?\w*、?\|?\w*[(（]?\w*[)）]?\w*)",
                                    data["CONTENT_"])
                if result:
                    fina_result = result[0].replace("|第二拟中标人", "")

            if not result:
                result = re.findall(r"(\w*[(（]?\w*[)）]?\w*),为第一成交候选人", data["CONTENT_"])
                if result:
                    fina_result = result[0]

            if not result:
                result = re.findall(r"\|1\|(\w*[(（]?\w*[)）]?\w*)\|", data["CONTENT_"])
                if result:
                    if ("公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0]
                            or "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0]
                            or "电视台" in result[0] or "报社广告中心" in result[0] or "报社" in result[0]
                            or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0]
                            or "研究院" in result[0] or "整治中心" in result[0] or "医院" in result[0]
                            or "园艺场" in result[0] or "开发局" in result[0] or "设计院" in result[0]):
                        fina_result = result[0]

                    else:
                        result = []

            if not result:
                result = re.findall(r"中[标选]人[:：|](\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])
                if result:
                    if result[0]:
                        fina_result = result[0]

            if not result:
                result = re.findall(r"[一二三四五六七八九十]、[成中][交标]结果(.*?)[一二三四五六七八九十]、", data["CONTENT_"])
                if result:
                    if "分包号：1" in result[0]:
                        if ("本分包流标" in result[0]) or ("废标原因" in result[0]):
                            fina_result = ""
                        else:
                            re_fina = re.findall(r"\|￥[^|]*\|([^|]*)", result[0])
                            if not re_fina:
                                re_fina = re.findall(r"\|￥[^|]*\|([^|]*)", data["CONTENT_"])
                            if re_fina:
                                fina_result = re_fina[0]
                            if ("流标" in fina_result) or (fina_result == "无"):
                                fina_result = ""

                    else:
                        re_result = re.findall(r"成?交?供应商名?称?：(\w*[(（]?\w*[)）]?\w*)", result[0])
                        if re_result:
                            if len(re_result) == 1:
                                fina_result = re_result[0]
                            else:
                                for r in re_result:
                                    if ("公司" in r or "经营部" in r or "勘测院" in r or "事务所" in r or "大学" in r
                                            or "车队" in r or "广播电台" in r or "电视台" in r or "报社广告中心" in r
                                            or "报社" in r or "代表处" in r or "规划院" in r or "出版社" in r
                                            or "研究院" in r or "整治中心" in r or "医院" in r or "园艺场" in r
                                            or "开发局" in r or "设计院" in r):
                                        fina_result = r

                        else:
                            re_result = re.findall(r"拟中标供应商：(\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                fina_result = re_result[0]

                        if not re_result:
                            re_result = re.findall(r"中标单位为(\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                fina_result = re_result[0]

                        if not re_result:
                            re_result = re.findall(r"(\w*[(（]?\w*[)）]?\w*)为中标人", result[0])
                            if re_result:
                                fina_result = re_result[0]

                        if not re_result:
                            re_result = re.findall(r"第一[成名]交?人?[：|](\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                fina_result = re_result[0]

                        if not re_result:
                            if "不足三家" in result[0] or "不足3家" in result[0] or "无三家投标" in result[0] \
                                    or "本次招标失败" in result[0] or "本次招标废标" or "无投标人" in result[0]:
                                re_result = ""
                                fina_result = ""

                        if not re_result:
                            re_result = re.findall(r"第一中标候选[单供][位应]商?[:：]?(\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                fina_result = re_result[0]

                        if not re_result:
                            re_result = re.findall(r"成交[单候][位选]人?：(\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                fina_result = re_result[0].replace("成交金额", "")

                        if not re_result:
                            re_result = re.findall(r"中标单位：(\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                fina_result = re_result[0]

                        if not re_result:
                            re_result = re.findall(r"\(入围供应商\)\|(\w*[(（]?\w*[)）]?\w*)", result[0])
                            if re_result:
                                if ("公司" in re_result[0] or "经营部" in re_result[0] or "勘测院" in re_result[0]
                                        or "事务所" in re_result[0] or "大学" in re_result[0] or "车队" in re_result[0]
                                        or "广播电台" in re_result[0] or "电视台" in re_result[0]
                                        or "报社广告中心" in re_result[0] or "报社" in re_result[0]
                                        or "代表处" in re_result[0] or "规划院" in re_result[0]
                                        or "出版社" in re_result[0] or "研究院" in re_result[0]
                                        or "整治中心" in re_result[0] or "医院" in re_result[0]
                                        or "园艺场" in re_result[0] or "开发局" in re_result[0]
                                        or "设计院" in re_result[0]):
                                    fina_result = re.sub("中标[金价]?[格额]?为?|成交[金折]?[额扣]?为?|由", "", re_result[0])
                                    fina_result = re.sub("[1234567890]*元?", "", fina_result)

                                    if len(fina_result) > 30:
                                        re_fina = re.findall("确定(\w*[(（]?\w*[)）]?\w*)为本次", fina_result)
                                        if re_fina:
                                            fina_result = re_fina[0]

                                else:
                                    re_result = []

                        if not re_result:
                            re_result = result[0].split("|")

                            i = 0
                            for r in re_result:
                                if ("公司" in r or "经营部" in r or "勘测院" in r or "事务所" in r or "大学" in r
                                        or "车队" in r or "广播电台" in r or "电视台" in r or "报社广告中心" in r
                                        or "报社" in r or "代表处" in r or "规划院" in r or "出版社" in r
                                        or "研究院" in r or "整治中心" in r or "医院" in r or "园艺场" in r
                                        or "开发局" in r or "设计院" in r):
                                    if "采购" in r or "项目" in r:
                                        continue
                                    else:
                                        fina_result = r
                                        i += 1

                            if i > 1:
                                fina_result = ""

                            if i == 0:
                                fina_result = ""

            if not result:
                result = re.findall(r"第一中标候选\|人\|名称：(\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])
                if result:
                    fina_result = result[0]

            if not result:
                result = re.findall(r"排名.*\|1\|(\w*[(（]?\w*[)）]?\w*)", data["CONTENT_"])
                if result:
                    fina_result = result[0]

            if not result:
                fina_result = ""

        if not fina_result:
            fina_result = ""

    data["WIN_CANDIDATE_"] = fina_result

    # 发布时间清洗
    if "NOTICE_TIME_" in data:
        if data["NOTICE_TIME_"]:
            if isinstance(data["NOTICE_TIME_"], str):
                if ("-" in data["NOTICE_TIME_"]) and ("{" not in data["NOTICE_TIME_"]) and (
                        "[" not in data["NOTICE_TIME_"]):
                    pass
                elif "年" in data["NOTICE_TIME_"] and ("至" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y年%m月%d日")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif "/" in data["NOTICE_TIME_"]:
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y/%m/%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif "\\" in data["NOTICE_TIME_"]:
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y\%m\%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("-" not in data["NOTICE_TIME_"]) and ("\\" not in data["NOTICE_TIME_"]) and (
                        "/" not in data["NOTICE_TIME_"]) and ("年" not in data["NOTICE_TIME_"]) and (
                        "CONTENT" not in data["NOTICE_TIME_"]) and ("公告" not in data["NOTICE_TIME_"]) and (
                        "作者" not in data["NOTICE_TIME_"]) and ("发布" not in data["NOTICE_TIME_"]) and (
                        "工程" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y%m%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("{" in data["NOTICE_TIME_"]) or ("[" in data["NOTICE_TIME_"]):
                    time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", data["NOTICE_TIME_"])
                    if time_result:
                        data["NOTICE_TIME_"] = time_result[0]
                else:
                    return None

            else:
                time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", str(data["NOTICE_TIME_"]))
                if time_result:
                    data["NOTICE_TIME_"] = time_result[0]
                else:
                    data["NOTICE_TIME_"] = ""
    if "NOTICE_TIME_" not in data:
        data["NOTICE_TIME_"] = ""

    return data


def run():
    shuffle_list = list()
    count = 0

    # 创建 MongoDB 查询数据库对象
    m_client = MongoClient(mongo_collection="CommonBidding")
    db, collection_list = m_client.client_to_mongodb()
    collection = m_client.get_check_collection(db=db, collection_list=collection_list)
    m_client.mongo_db = "spider_data"
    m_client.mongo_entity_code = "500000CQGGZY"

    try:
        mongo_data_list = m_client.search_title_from_mongodb(collection)
    except pymongo.errors.ServerSelectionTimeoutError:
        print("正在重新连接")
        time.sleep(1)
        mongo_data_list = m_client.search_title_from_mongodb(collection)

    for data in mongo_data_list:
        data_list = data_shuffle(data)


if __name__ == '__main__':
    run()
