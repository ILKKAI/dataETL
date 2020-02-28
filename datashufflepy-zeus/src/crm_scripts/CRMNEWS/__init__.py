# -*- coding: utf-8 -*-
import copy
import os
import re
import sys
import time


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-12])

from crm_scripts import GenericScript
from tools.req_for_api import req_for_serial_number
from tools.req_for_ai import req_for_ner
from __config import TABLE_NAME


class BranchNews(GenericScript):

    def __init__(self, table_name, collection_name, param):
        # verify_field = { hbase: mongo}
        super(BranchNews, self).__init__(table_name=table_name, collection_name=collection_name, param=param, verify_field={'DATAS_SOURCE_URL_': 'URL_'})
        # 导入本地的脚本包
        # self.script_path = self.m_client.mongo_collection

    def generic_shuffle(self, data, field="CONTENT_"):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        # different shuffle rule
        re_data = copy.deepcopy(data)
        serial_number = req_for_serial_number(code="CRM_NEWS")
        re_data["ID_"] = serial_number

        # 作者
        if "NEWS_AUTHOR_" in data:
            if "编辑" in data["NEWS_AUTHOR_"]:
                re_data["NEWS_AUTHOR_"] = re.findall(r"编辑[:：](\w+)", data["NEWS_AUTHOR_"])[0]

        # 内容
        re_data["NEWS_DESC_TEXT_"] = re.sub(r"(var.*?;\|)(?![a-zA-Z])", "", data["NEWS_DESC_TEXT_"]).replace("|", "")

        # 调用模型  -- 实体识别
        try:
            res = req_for_ner(text=re_data["NEWS_DESC_TEXT_"])
        except Exception as e:
            self.logger.exception(f"2.2--err: 请求模型 req_for_credit_relative 错误."
                                  f" 原始数据 collection = {self.m_client.mongo_collection};"
                                  f" ENTITY_CODE_ = {self.entity_code};"
                                  f" 原始数据 _id = {data['_id']};"
                                  f" error: {e}.")
        else:
            if res.get("Organ"):
                bank_name = res.get("Organ").get("entity")
                if bank_name and '银行' in bank_name:
                    re_data["BANK_NAME_"] = bank_name
                    bank_list = list()
                    bank_code_list = list()
                    for each in self.bank_list:
                        if re_data.get('BANK_NAME_') in each['ALIAS_']:
                            bank_list.append(each["NAME_"])
                            bank_code_list.append(each["CODE_"])
                    if bank_list:
                        re_data["BANK_NAME_"] = "|".join(bank_list)
                    if bank_code_list:
                        re_data["BANK_CODE_"] = "|".join(bank_code_list)

        return [{"TABLE_NAME_": 'CRM_NEWS', "DATA_": re_data}]


if __name__ == '__main__':
    # param = sys.argv[1]
    '''
    CRMJPFX_ZXYQ_XLCJ_JRBGT
    CRMJPFX_ZXYQ_XLCJ_JRTS
    CRMJPFX_ZXYQ_XL_HMTS
    CRMJPFX_ZXYQ_XLCJ_GSDT
    '''
    for _ in range(100):
        try:
            # param = "{'entityType':'CRMNEWS','limitNumber':10000,'entityCode':['CRMJPFX_ZXYQ_XLCJ_GSDT','CRMJPFX_ZXYQ_XL_HMTS', 'CRMJPFX_ZXYQ_XLCJ_JRTS','CRMJPFX_ZXYQ_XLCJ_JRBGT', ]}"
            param = "{'entityType':'CRMNEWS','limitNumber':10000,'entityCode':['CRMJPFX_ZXYQ_XL_HMTS' ]}"

            script = BranchNews(table_name=TABLE_NAME("CRM_NEWS"), collection_name="CRMJPFX_ZXYQ", param=param)
            script.main()
            script.close_client()
        except:
            continue
