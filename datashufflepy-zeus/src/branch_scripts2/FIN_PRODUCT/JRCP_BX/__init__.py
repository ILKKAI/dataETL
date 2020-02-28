# -*- coding: utf-8 -*-
'''
执行
'''
import os
import sys
import re


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-27])

from branch_scripts2 import GenericScript
from tools.req_for_api import req_for_serial_number, req_for_something, req_for_file_save
from __config import TABLE_NAME


class BranchInsurance(GenericScript):

    def __init__(self, table_name, collection_name, param, verify_field):

        super(BranchInsurance, self).__init__(table_name=table_name, collection_name=collection_name, param=param, verify_field=verify_field)
        self.mysql_client.mysql_table = "cha_insurance_company"
        self.company_list = self.mysql_client.search_from_mysql(connection=self.mysql_connection, output=["CODE_", "NAME_", "ALIAS_"])
        # print(self.company_list)
        # 缴费方式及code
        self.pay_type = {"期缴": "QJ", "趸缴": "DJ", "其他": "QT"}

    def generic_shuffle(self, data, field="PRO_NAME_"):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        # different shuffle rule
        # 如果data是一个list
        if isinstance(data, list):
            re_data_list = []
            for item in data:
                re_data_list.append({"TABLE_NAME_": self.script_name, "DATA_": self.generic_shuffle(item)})
            return re_data_list

        re_data = dict()
        serial_number = req_for_serial_number(code="JRCP_BX")
        re_data["ID_"] = serial_number + "TEST"
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        re_data["SOURCE_"] = source[0]
        re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"]
        re_data["VERSION_"] = "0"
        re_data["DATA_VERSION_"] = "0"
        # todo
        re_data["SOURCE_TYPE_"] = ""

        # 模型

        re_data["HOT_"] = data["HOT_"] if "HOT_" in data else "0"

        re_data["PRO_NAME_"] = data["PRO_NAME_"]

        # 保险公司
        if "COM_NAME_" in data:
            for each in self.company_list:
                if each["NAME_"]:
                    if data["COM_NAME_"] in each["NAME_"] or each["NAME_"] in data["COM_NAME_"]:
                        re_data["COM_NAME_"] = each["NAME_"]
                        re_data["COM_NAME_CODE_"] = each["CODE_"]
                    elif each["ALIAS_"] and data["COM_NAME_"] in each["ALIAS_"]:
                        re_data["COM_NAME_"] = each["NAME_"]
                        re_data["COM_NAME_CODE_"] = each["CODE_"]
            if "COM_NAME_" not in re_data:
                re_data["COM_NAME_"] = data["COM_NAME_"]

        # 保额 补录
        if "ENSURE_PRICE_" in data:
            re_data["ENSURE_PRICE_"] = data["ENSURE_PRICE_"]
        # else:
        #     re_data["ENSURE_PRICE_"] = [100000, 500000, 1000000][random.randint(0, 2)]
        # 保费 补录
        if "ENSURE_FEE_" in data:
            re_data["ENSURE_FEE_"] = data["ENSURE_FEE_"]
        # else:
        #     re_data["ENSURE_FEE_"] = [50, 100, 200, 150][random.randint(0, 3)]
        # 产品特色 补录
        if "SPECAIL_" in data:
            re_data["SPECAIL_"] = data["SPECAIL_"]
        # 产品简介 补录
        if "BRIEF_" in data:
            re_data["BRIEF_"] = data["BRIEF_"]
        # 承保年龄 补录
        if "AGE_" in data:
            re_data["AGE_"] = data["AGE_"]
        # else:
        #     re_data["AGE_"] = [50, 70, 60, 80][random.randint(0, 3)]
        # 保险期间 补录
        if "ENSURE_DATE_" in data:
            re_data["ENSURE_DATE_"] = data["ENSURE_DATE_"]
        # else:
        #     re_data["ENSURE_DATE_"] = ["至80岁", "至60岁", "一年", "五年", "十年", "终身"][random.randint(0, 5)]
        # 投保份数 补录
        if "BUY_LIMIT_" in data:
            re_data["BUY_LIMIT_"] = data["BUY_LIMIT_"]
        # else:
        #     re_data["BUY_LIMIT_"] = [1, 2, "不限"][random.randint(0, 2)]
        # 保单形式 补录
        if "ENSURE_MODE_" in data:
            re_data["ENSURE_MODE_"] = data["ENSURE_MODE_"]
        # 保单 补录
        if "ENSURE_MODE_CODE_" in data:
            re_data["ENSURE_MODE_CODE_"] = data["ENSURE_MODE_CODE_"]
        # 适用人群 补录
        if "SUIT_" in data:
            re_data["SUIT_"] = data["SUIT_"]
        # else:
        #     re_data["SUIT_"] = ["20岁以下", "20岁至50岁人群", "无重大疾病隐患者", "不限"][random.randint(0,3)]
        # 原始保险分类 补录
        if "ENSURE_SOURCE_TYPE_" in data:
            re_data["ENSURE_SOURCE_TYPE_"] = data["ENSURE_SOURCE_TYPE_"]
        # 保险类型 补录
        # type_dict = {"寿险": "SX", "年金险": "NJX", "意外险": "YWX", "个人财险": "GRCX", "企业财险": "QYCX", "旅游险": "LYX", "健康险": "JKX", "理财险": "LCX"}
        if "ENSURE_TYPE_" in data:
            re_data["ENSURE_TYPE_"] = data["ENSURE_TYPE_"]
            # re_data["ENSURE_TYPE_"] = ["寿险", "年金险", "意外险", "个人财险", "企业财险", "旅游险", "健康险", "理财险"][random.randint(0, 7)]
        # 保险类型分类 补录
        # if 1:
        if "ENSURE_TYPE_CODE_" in data:
            re_data["ENSURE_TYPE_CODE_"] = data["ENSURE_TYPE_CODE_"]
            # re_data["ENSURE_TYPE_CODE_"] = type_dict[re_data["ENSURE_TYPE_"]]
        # 推荐
        re_data["RECOMMEND_"] = "N"
        # 畅销
        re_data["GOOD_SALE_"] = "N"
        # 最新
        re_data["NEW_SALE_"] = "N"
        # 保障内容 补录
        if "ENSURE_CONTENT_" in data:
            re_data["ENSURE_CONTENT_"] = data["ENSURE_CONTENT_"]
        # 投保须知 补录
        if "NOTICE_" in data:
            re_data["NOTICE_"] = data["NOTICE_"]
        # 产品介绍 补录
        if "PRO_DETAIL_" in data:
            re_data["PRO_DETAIL_"] = data["PRO_DETAIL_"]
        if "ENSURE_PAY_" in data.keys():
            re_data["ENSURE_PAY_"] = data["ENSURE_PAY_"].strip().replace("交", "缴")
            if re_data["ENSURE_PAY_"] not in self.pay_type:
                re_data["ENSURE_PAY_"] = "其他"
            re_data["ENSURE_PAY_CODE_"] = self.pay_type[re_data["ENSURE_PAY_"]]
        # 如果没有缴费方式从产品名字中再获取一次
        else:
            if re.findall(r"期[缴交]", data["PRO_NAME_"]):
                re_data["ENSURE_PAY_"] = "期缴"
                re_data["ENSURE_PAY_CODE_"] = "QJ"
            elif re.findall(r"趸[缴交]", data["PRO_NAME_"]):
                re_data["ENSURE_PAY_"] = "趸缴"
                re_data["ENSURE_PAY_CODE_"] = "DJ"
        # FDFS上传
        if "LOCAL_PDF_PATH_" in data:
            try:
                p_response = req_for_file_save(id=re_data["ID_"], type_code=f"CHA_INSURANCE_PDF",
                                               file_name=data["LOCAL_PDF_NAME_"], postfix="pdf",
                                               file=open(data["LOCAL_PDF_PATH_"], "rb"))
                p_response.close()
            except Exception as e:
                self.logger.warning(f"_id: {data['_id']},文件上传失败, ERROR: {e}")
        if "WORD_" in data:
            try:
                response = req_for_something(url=data["WORD_"])
            except Exception as e:
                self.logger.warning(f"_id: {data['_id']},获取PDF失败, ERROR: {e}")
            else:
                if response:
                    try:
                        p_response = req_for_file_save(id=re_data["ID_"], type_code=f"CHA_INSURANCE_WORD",
                                                       file_name=data["PDF_NAME_"].replace(".doc", ""), postfix="doc",
                                                       file=response.content)
                        self.logger.info(f"{p_response.content.decode('utf-8')}")
                        p_response.close()
                    except Exception as e:
                        self.logger.warning(f"_id: {data['_id']},文件上传失败, ERROR: {e}")
                    finally:
                        response.close()
                else:
                    self.logger.warning(f'id: {data["_id"]},获取PDF失败')

        if "HTML_" in data:
            del data["HTML_"]
        re_data = super(BranchInsurance, self).generic_shuffle(data=data, re_data=re_data, field="ENTITY_NAME_")
        re_data["PUBLISH_TIME_"] = re_data["SPIDER_TIME_"]
        return [{"TABLE_NAME_": self.script_name, "DATA_": re_data}]


if __name__ == '__main__':
    # verify_field = {'URL_': 'URL_', 'PRO_NAME_': 'PRO_NAME_'}
    verify_field = {'URL_': 'URL_'}
    param = "{'entityType':'JRCP_BX','limitNumber':1000,'entityCode':['JRCP_BX_HEBYH_GW_ALL']}"

    script = BranchInsurance(table_name=TABLE_NAME("CHA_BRANCH_INSURANCE"), collection_name="JRCP_BX", param=param, verify_field=verify_field)
    script.main()
    script.close_client()
