# -*- coding: utf-8 -*-

# 中国金融集中采购网（金采网）网站 86JCW  946
# 无 WIN_CANDIDATE_ 字段
# 发布时间正确 5条 NOTICE_TIME_ 为"" ， 原网页链接失效  http://www.cfcpn.com/front/newcontext/28647

import re

import time

from database._phoenix_hbase import PhoenixHbase
from scripts import GenericScript


# def data_shuffle(mongo_data_list):
#     data_list = list()
#     notice = 0
#     for data in mongo_data_list:
#         # 中标人清洗
#         fina_result = ""
#         result = re.findall(r"第一[中入成]?[标围交]?\|?候?选?人[为|]?[:：]"
#                             r"\|?[投中]?标?候?选?人?名?称?：?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#         if result:
#             if len(result) == 1:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0].replace("投标报价", "")
#                 else:
#                     result = []
#             else:
#                 result = []
#         if not result:
#             result = re.findall(r"排名第一的[入中][围标][供候][应选][商人]：\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0].replace("投标报价", "")
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"第一[中入成]?[标围交]?\|?候?选?单位名?称?为?[:：|]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
#                                 data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         if "入围" in result[0]:
#                             fina_result = re.sub("入围.*", "", result[0])
#                         else:
#                             fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"第一\|?[中入成][标围交]候?选?人名?称?为?[|:：]?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0].replace("为本项目中标人", "")
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"第[一1]名\w*?[:：|]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         if "第二名" not in result[0]:
#                             fina_result = result[0]
#                         else:
#                             result = []
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"[成中][交选标][供候侯][应选][商人]名?称?为?[:：]\|?（?1?）?\|?[单投第中]?[位标一]?人?名?称?：?\|?"
#                                 r"(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "万欣和" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"中[标选][单人]位?名?称?为?[:：]\|?1?\|?、?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
#                                 data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0].replace("2", "")
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"中标方案[:：](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"服务方[:：](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(
#                 r"[候入]?[选围]?入?围?[供服][应务]商名?称?为?[:：|]\|?1?\|?[、.]?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
#                 data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         if "已在实施" in result[0]:
#                             fina_result = ""
#                         else:
#                             fina_result = result[0].replace("2", "")
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"成交单位[:：]\|?单位名称[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"中标结果[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0].replace("（投标报价", "")
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"候选人[:：]\|?（?1?）?单?位?名?称?[:：]?\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
#                                 data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"致[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(
#                 r"[成中入][交选标围][供候侯]?[应选]?[商单]位?名?称?为?[:：|]\|?1?\|?、?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
#                 data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"中[标选]单位名?称?及中?[标选]?金额\|(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"\|1\|.*?\|2?\|?", data["CONTENT_"])
#             if result:
#                 re_result = str(result).split("|")
#                 i = 0
#                 for r in re_result:
#                     if "公司" in r or "经营部" in r or "勘测院" in r or "事务所" in r or \
#                             "大学" in r or "车队" in r or "广播电台" in r or "电视台" in r or \
#                             "报社" in r or "代表处" in r or "规划院" in r or "出版社" in r or \
#                             "研究院" in r or "中心" in r or "医院" in r or "园艺场" in r or \
#                             "开发局" in r or "设计院" in r or "饭店" in r or "门市" in r or \
#                             "集团" in r or "酒店" in r:
#                         fina_result = r
#                         i += 1
#
#                 if i > 1:
#                     fina_result = ""
#                     result = []
#                 elif i == 0:
#                     result = []
#         if not result:
#             result = re.findall(r"中标\w*?[:：]\|?(\w*)", data["CONTENT_"])
#             if result:
#                 if len(result) == 1:
#                     if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                             "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                             "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                             "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                             "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                             "集团" in result[0] or "酒店" in result[0] or "园林" in result[0]:
#                         fina_result = result[0]
#                     else:
#                         result = []
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"入围供应商信息[:：]\|1[、.](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0]
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"确定“(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)”为预中标单位", data["CONTENT_"])
#             if result:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0]
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"授予合同单位[:：](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0]
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"评标结果如下[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0]
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"\|（1）(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)\|", data["CONTENT_"])
#             if result:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0]
#                 else:
#                     result = []
#         if not result:
#             result = re.findall(r"发票类型\|(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
#             if result:
#                 if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
#                         "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
#                         "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
#                         "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
#                         "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
#                         "集团" in result[0] or "酒店" in result[0]:
#                     fina_result = result[0]
#                 else:
#                     result = []
#         # todo any other wrong candidate
#         if ("项目" in fina_result) or ("申请人" in fina_result) or ("至今" in fina_result) or ("合作" in fina_result) or ("代表人" in fina_result) or ("采购" in fina_result):
#             fina_result = ""
#
#         data["WIN_CANDIDATE_"] = fina_result
#
#         # 发布时间清洗
#         if "NOTICE_TIME_" in data:
#             if data["NOTICE_TIME_"]:
#                 if isinstance(data["NOTICE_TIME_"], str):
#                     if ("-" in data["NOTICE_TIME_"]) and ("{" not in data["NOTICE_TIME_"]) and (
#                             "[" not in data["NOTICE_TIME_"]):
#                         pass
#                     elif "年" in data["NOTICE_TIME_"] and ("至" not in data["NOTICE_TIME_"]):
#                         time_array = time.strptime(data["NOTICE_TIME_"], "%Y年%m月%d日")
#                         data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)
#
#                     elif "/" in data["NOTICE_TIME_"]:
#                         time_array = time.strptime(data["NOTICE_TIME_"], "%Y/%m/%d")
#                         data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)
#
#                     elif "\\" in data["NOTICE_TIME_"]:
#                         time_array = time.strptime(data["NOTICE_TIME_"], "%Y\%m\%d")
#                         data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)
#
#                     elif ("-" not in data["NOTICE_TIME_"]) and ("\\" not in data["NOTICE_TIME_"]) and (
#                             "/" not in data["NOTICE_TIME_"]) and ("年" not in data["NOTICE_TIME_"]) and (
#                             "CONTENT" not in data["NOTICE_TIME_"]) and ("公告" not in data["NOTICE_TIME_"]) and (
#                             "作者" not in data["NOTICE_TIME_"]) and ("发布" not in data["NOTICE_TIME_"]) and (
#                             "工程" not in data["NOTICE_TIME_"]) and ("开始" not in data["NOTICE_TIME_"]):
#                         time_array = time.strptime(data["NOTICE_TIME_"], "%Y%m%d")
#                         data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)
#
#                     elif ("{" in data["NOTICE_TIME_"]) or ("[" in data["NOTICE_TIME_"]):
#                         time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", data["NOTICE_TIME_"])
#                         if time_result:
#                             data["NOTICE_TIME_"] = time_result[0]
#                     else:
#                         continue
#
#                 else:
#                     time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", str(data["NOTICE_TIME_"]))
#                     if time_result:
#                         data["NOTICE_TIME_"] = time_result[0]
#                     else:
#                         data["NOTICE_TIME_"] = ""
#         if "NOTICE_TIME_" not in data:
#             data["NOTICE_TIME_"] = ""
#
#         # CONTENT_ 内容清洗
#         data["CONTENT_"] = re.sub(r"</?[?a-zA-Z!].*?>|&nbsp;", "", str(data["CONTENT_"]))
#
#         data_list.append(data)
#
#     return data_list


def data_shuffle(data):
    data_list = list()
    notice = 0
    # 中标人清洗
    fina_result = ""
    result = re.findall(r"第一[中入成]?[标围交]?\|?候?选?人[为|]?[:：]"
                        r"\|?[投中]?标?候?选?人?名?称?：?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
    if result:
        if len(result) == 1:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0].replace("投标报价", "")
            else:
                result = []
        else:
            result = []
    if not result:
        result = re.findall(r"排名第一的[入中][围标][供候][应选][商人]：\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0].replace("投标报价", "")
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"第一[中入成]?[标围交]?\|?候?选?单位名?称?为?[:：|]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
                            data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    if "入围" in result[0]:
                        fina_result = re.sub("入围.*", "", result[0])
                    else:
                        fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"第一\|?[中入成][标围交]候?选?人名?称?为?[|:：]?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0].replace("为本项目中标人", "")
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"第[一1]名\w*?[:：|]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    if "第二名" not in result[0]:
                        fina_result = result[0]
                    else:
                        result = []
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"[成中][交选标][供候侯][应选][商人]名?称?为?[:：]\|?（?1?）?\|?[单投第中]?[位标一]?人?名?称?：?\|?"
                            r"(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "万欣和" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"中[标选][单人]位?名?称?为?[:：]\|?1?\|?、?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
                            data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0].replace("2", "")
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"中标方案[:：](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"服务方[:：](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(
            r"[候入]?[选围]?入?围?[供服][应务]商名?称?为?[:：|]\|?1?\|?[、.]?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
            data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    if "已在实施" in result[0]:
                        fina_result = ""
                    else:
                        fina_result = result[0].replace("2", "")
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"成交单位[:：]\|?单位名称[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"中标结果[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0].replace("（投标报价", "")
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"候选人[:：]\|?（?1?）?单?位?名?称?[:：]?\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
                            data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"致[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(
            r"[成中入][交选标围][供候侯]?[应选]?[商单]位?名?称?为?[:：|]\|?1?\|?、?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)",
            data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"中[标选]单位名?称?及中?[标选]?金额\|(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    # if not result:
    #     result = re.findall(r"\|1\|.*?\|2?\|?", data["CONTENT_"])
    #     if result:
    #         re_result = str(result).split("|")
    #         i = 0
    #         for r in re_result:
    #             if "公司" in r or "经营部" in r or "勘测院" in r or "事务所" in r or \
    #                     "大学" in r or "车队" in r or "广播电台" in r or "电视台" in r or \
    #                     "报社" in r or "代表处" in r or "规划院" in r or "出版社" in r or \
    #                     "研究院" in r or "中心" in r or "医院" in r or "园艺场" in r or \
    #                     "开发局" in r or "设计院" in r or "饭店" in r or "门市" in r or \
    #                     "集团" in r or "酒店" in r:
    #                 fina_result = r
    #                 i += 1
    #
    #         if i > 1:
    #             fina_result = ""
    #             result = []
    #         elif i == 0:
    #             result = []
    if not result:
        result = re.findall(r"中标\w*?[:：]\|?(\w*)", data["CONTENT_"])
        if result:
            if len(result) == 1:
                if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                        "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                        "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                        "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                        "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                        "集团" in result[0] or "酒店" in result[0] or "园林" in result[0]:
                    fina_result = result[0]
                else:
                    result = []
            else:
                result = []
    if not result:
        result = re.findall(r"入围供应商信息[:：]\|1[、.](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0]
            else:
                result = []
    if not result:
        result = re.findall(r"确定“(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)”为预中标单位", data["CONTENT_"])
        if result:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0]
            else:
                result = []
    if not result:
        result = re.findall(r"授予合同单位[:：](\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0]
            else:
                result = []
    if not result:
        result = re.findall(r"评标结果如下[:：]\|?(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0]
            else:
                result = []
    if not result:
        result = re.findall(r"\|（1）(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)\|", data["CONTENT_"])
        if result:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0]
            else:
                result = []
    if not result:
        result = re.findall(r"发票类型\|(\w*-?\w*[(（《]?\w*-?\w*[)）》]?\w*-?\w*)", data["CONTENT_"])
        if result:
            if "公司" in result[0] or "经营部" in result[0] or "勘测院" in result[0] or "事务所" in result[0] or \
                    "大学" in result[0] or "车队" in result[0] or "广播电台" in result[0] or "电视台" in result[0] or \
                    "报社" in result[0] or "代表处" in result[0] or "规划院" in result[0] or "出版社" in result[0] or \
                    "研究院" in result[0] or "中心" in result[0] or "医院" in result[0] or "园艺场" in result[0] or \
                    "开发局" in result[0] or "设计院" in result[0] or "饭店" in result[0] or "门市" in result[0] or \
                    "集团" in result[0] or "酒店" in result[0]:
                fina_result = result[0]
            else:
                result = []
    # todo any other wrong candidate
    if ("项目" in fina_result) or ("申请人" in fina_result) or ("至今" in fina_result) or ("合作" in fina_result) or ("代表人" in fina_result) or ("采购" in fina_result):
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
                        "工程" not in data["NOTICE_TIME_"]) and ("开始" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y%m%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("{" in data["NOTICE_TIME_"]) or ("[" in data["NOTICE_TIME_"]):
                    time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", data["NOTICE_TIME_"])
                    if time_result:
                        data["NOTICE_TIME_"] = time_result[0]
                else:
                    pass

            else:
                time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", str(data["NOTICE_TIME_"]))
                if time_result:
                    data["NOTICE_TIME_"] = time_result[0]
                else:
                    data["NOTICE_TIME_"] = ""
    if "NOTICE_TIME_" not in data:
        data["NOTICE_TIME_"] = ""

    # CONTENT_ 内容清洗
    data["CONTENT_"] = re.sub(r"</?[?a-zA-Z!].*?>|&nbsp;", "", str(data["CONTENT_"]))

    return data


def run():
    script = GenericScript(entity_code="86JCW", entity_type="CommonBidding")

    # 从 MongoDB 获取数据
    mongo_data_list = script.data_from_mongo()
    data_list = data_shuffle(mongo_data_list)
    # # 创建 Phoenix 对象
    # p_client = PhoenixHbase(table_name="CommonBidding")
    # # 连接 Phoenix
    # connection = p_client.connect_to_phoenix()
    # # 插入数据
    # p_client.upsert_to_phoenix(connection=connection, data_list=data_list)
    # # 关闭连接
    # p_client.close_client_phoenix(connection=connection)


if __name__ == '__main__':
    run()
