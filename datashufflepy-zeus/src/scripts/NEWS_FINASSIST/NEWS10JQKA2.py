# -*- coding: utf-8 -*-

# 同花顺财经股市新闻二 NEWS10JQKA2

import hashlib
import re
import pandas as pd
from scripts import GenericScript

#大量内容为空，删除还是加空
def data_shuffle(mongo_data_list):
    data_list = list()

    count = 0

    pattern = re.compile(r"vartc(.*?)\|")

    for data in mongo_data_list:
        re_data = dict()

        dict_Bank = {'中国工商银行': 'ICBC', '工商银行': 'ICBC', '工行': 'ICBC', '中国农业银行': 'ICBC', '农业银行': 'ABC', '农行': 'ABC',
                     '中国银行': 'BOC', '中行': 'BOC', '中国建设银行': 'CCB',
                     '建设银行': 'CCB', '建行': 'CCB', '交通银行': 'BOCOM', '交行': 'BOCOM', '中国邮政储蓄银行': 'PSBC', '邮政储蓄银行': 'PSBC',
                     '邮政银行': 'PSBC', '邮蓄银行': 'PSBC', '浙商银行': 'CZB', '渤海银行': 'CBHB',
                     '中信银行': 'ECITIC', '中国光大银行': 'CEB', '光大银行': 'CEB', '华夏银行': 'HXB', '中国民生银行': 'CMBC', '民生银行': 'CMBC',
                     '招商银行': 'CMB', '招行': 'CMB', '兴业银行': 'CIB', '广发银行': 'CGB', '平安银行': 'PAB',
                     '上海浦东发展银行': 'SPDB', '浦东发展银行': 'SPDB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL'}

        # 公有列族 "C"
        # 实体编码
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        # 实体名字-后续添加的
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        # 原网页地址
        re_data["URL_"] = data["URL_"]
        # 时间维度编码(发布时间)
        if data["TIME_"]:
            if "来源" in data["TIME_"]:
                index = data["TIME_"].index("来源")
                re_data["PERIOD_CODE_"] = data["TIME_"][:index][:10]
            else:
                re_data["PERIOD_CODE_"] = data["TIME_"][:10]
        else:
            re_data["PERIOD_CODE_"] = ""
        re_data["PERIOD_CODE_"] = re_data["PERIOD_CODE_"].replace("-", "")
        # 新闻发布时间
        if data["TIME_"]:
            if "来" in data["TIME_"]:
                index = data["TIME_"].index("来")
                re_data["PUBLISH_TIME_"] = data["TIME_"][:index]
                if len(re_data["PUBLISH_TIME_"]) == 15 or len(re_data["PUBLISH_TIME_"]) == 18:
                    re_data["PUBLISH_TIME_"] = re_data["PUBLISH_TIME_"][:10] + " " + re_data["PUBLISH_TIME_"][10:15]
                else:
                    re_data["PUBLISH_TIME_"] = data["TIME_"][:-3]
            else:
                if len(data["TIME_"]) == 15:
                    re_data["PUBLISH_TIME_"] = data["TIME_"][:10] + " " + data["TIME_"][10:15]
                else:
                    re_data["PUBLISH_TIME_"] = data["TIME_"][:-3]
        else:
            re_data["PUBLISH_TIME_"] = ""
        # 数据处理状态， 统一为1
        re_data["STATUS_"] = "1"
        # 备注
        re_data["REMARK_"] = ""
        # 创建时间
        re_data["CREATE_TIME_"] = data["DATETIME_"]
        # 更新时间
        re_data["UPDATE_TIME_"] = ""

        # 新闻无以下信息
        # re_data["AREA_CODE_"] = ""
        # re_data["BANK_CODE_"] = ""
        # re_data["BANK_NAME_"] = ""
        re_data["BANK_NAME_"] = ""
        re_data["BANK_CODE_"] = ""
        dict_Bank_Keys = dict_Bank.keys()
        str_Bank = ""
        str_Bank_Code = ""
        for dict_Bank_Key in dict_Bank_Keys:
            if dict_Bank_Key in data["CONTENT_"]:
                str_Bank_Name = str_Bank + dict_Bank_Key + "|"
                str_Bank_Code = str_Bank_Code + dict_Bank[dict_Bank_Key] + "|"
                re_data["BANK_NAME_"] = str_Bank_Name
                re_data["BANK_CODE_"] = str_Bank_Code

        re_data["UNIT_CODE_"] = ""

        # 私有列族 "F"
        # 新闻标题
        if data["TITLE_"]:
            re_data["TITLE_"] = data["TITLE_"]
        else:
            re_data["TITLE_"] = ""

        # 简介
        re_data["BRIEF_"] = ""

        # 关键词
        re_data["KEYWORDS_"] = ""

        # 内容-数据未处理
        if data["CONTENT_"]:
            index = data["CONTENT_"].find("|varc")
            if index != -1:
                re_data["CONTENT_"] = data["CONTENT_"][:index]
                re_ = pattern.search(data["CONTENT_"])
                if re_ != None:
                    if re_.group() in data["CONTENT_"]:
                        re_data["CONTENT_"] = data["CONTENT_"].replace(re_.group(), "")
                    else:
                        re_data["CONTENT_"] = data["CONTENT_"]
                    index_1 = re_data["CONTENT_"].find("|(fun")
                    if index_1:
                        re_data["CONTENT_"] = re_data["CONTENT_"][:index_1]
                else:
                    index_1 = data["CONTENT_"].find("|(fun")
                    if index_1:
                        re_data["CONTENT_"] = data["CONTENT_"][:index_1]
                    else:
                        re_data["CONTENT_"] = data["CONTENT_"]
            else:
                re_ = pattern.search(data["CONTENT_"])
                if re_ != None:
                    if re_.group() in data["CONTENT_"]:
                        re_data["CONTENT_"] = data["CONTENT_"].replace(re_.group(), "")
                    else:
                        re_data["CONTENT_"] = data["CONTENT_"]
                    index_1 = re_data["CONTENT_"].find("|(fun")
                    if index_1:
                        re_data["CONTENT_"] = re_data["CONTENT_"][:index_1]
                else:
                    index_1 = data["CONTENT_"].find("|(fun")
                    if index_1:
                        re_data["CONTENT_"] = data["CONTENT_"][:index_1]
                    else:
                        re_data["CONTENT_"] = data["CONTENT_"]
        else:
            re_data["CONTENT_"] = ""

        # 新闻来源
        if data["DATA_SOURCE_"]:
            if "iVB" in data["DATA_SOURCE_"]:
                re_data["DATA_SOURCE_"] = ""
            else:
                re_data["DATA_SOURCE_"] = data["DATA_SOURCE_"]
        elif "讯" in data["CONTENT_"][:15]:
            fin = data["CONTENT_"][:15]
            fin_index = fin.find("讯")
            re_data["DATA_SOURCE_"] = data["CONTENT_"][:fin_index]
            if "【" in re_data["DATA_SOURCE_"]:
                re_data["DATA_SOURCE_"] = data["DATA_SOURCE_"][1:]
            if "11月" in re_data["DATA_SOURCE_"]:
                index = re_data["DATA_SOURCE_"].find("11月")
                re_data["DATA_SOURCE_"] = data["DATA_SOURCE_"][:index]
            if "12月" in re_data["DATA_SOURCE_"]:
                index = re_data["DATA_SOURCE_"].find("12月")
                re_data["DATA_SOURCE_"] = data["DATA_SOURCE_"][:index]
        elif "新华社" in data["CONTENT_"][:10]:
            re_data["DATA_SOURCE_"] = "新华网"
        else:
            re_data["DATA_SOURCE_"] = ""

        if "财经要闻宏观经济产经新闻国际财经金融市场财经评论" in re_data["CONTENT_"]:
            continue
        if not re_data["CONTENT_"]:
            continue

        # re_data["CONTENT_"] = ""

        # ID
        hash_m = hashlib.md5()
        hash_m.update(data["ENTITY_NAME_"].encode("utf-8"))
        hash_title = hash_m.hexdigest()
        re_data["ID_"] = data["ENTITY_CODE_"] + "_" + str(9999999999 - int(float(data["DEALTIME_"]))) + "_" + str(
            hash_title)

        # print("1")
        # print(data["TIME_"])
        # print(re_data["TIME_"])
        # print(re_data["PERIOD_CODE_"])
        data_list.append(re_data)

    return data_list



def run():
    # entity_code 为 当前实体编码， entity_type 为当前实体所属类别， 对应 MongoDB 中集合名称
    script = GenericScript(entity_code="NEWS10JQKA2", entity_type="NEWS_FINASSIST")

    # 调用 GenericScript.data_from_mongo() 方法获取数据
    mongo_data_list = script.data_from_mongo()

    data_list = data_shuffle(mongo_data_list)

    # pd.DataFrame(data_list).to_csv('E:\\NEWS_CLEAN_\\NEWS10JQKA2.csv', encoding="utf_8_sig")
    # print("文件导出成功")

    # 创建 Phoenix 对象
    # p_client = PhoenixHbase(table_name="CommonBidding")
    # 连接 Phoenix
    # connection = p_client.connect_to_phoenix()
    # todo 插入数据


if __name__ == '__main__':
    run()
