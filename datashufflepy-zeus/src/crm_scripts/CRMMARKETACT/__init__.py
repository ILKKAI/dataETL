# -*- coding: utf-8 -*-
import os
import re
import sys
import time


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-12])

from tools.req_for_api import req_for_serial_number
from crm_scripts import GenericScript
from __config import TABLE_NAME, CREATE_ID, CREATE_NAME


class BranchOrganize(GenericScript):

    def __init__(self, table_name, collection_name, param):
        super(BranchOrganize, self).__init__(table_name, collection_name, param, verify_field={"active_name": "active_name",})

    def __shuffle(self, data):
        serial_number = req_for_serial_number(code="CRM_MARKET_ACT")
        data["ID_"] = serial_number

        bank_list = list()
        bank_code_list = list()
        for each in self.bank_list:
            if data.get('BANK_NAME_') in each['ALIAS_']:
                bank_list.append(each["NAME_"])
                bank_code_list.append(each["CODE_"])
        if bank_list:
            data["BANK_NAME_"] = "|".join(bank_list)
        if bank_code_list:
            data["BANK_CODE_"] = "|".join(bank_code_list)

        return data

    def generic_shuffle(self, data, field="PRO_NAME_"):
        """
        通用清洗规则写这里, 如不需要通用清洗规则则不继承重写
        :param data:
        :param field:
        :return:
        """
        if isinstance(data, dict):
            re_data = self.__shuffle(data)
            return [{"TABLE_NAME_": self.script_name, "DATA_": re_data}]
        elif isinstance(data, list):
            re_list = list()
            for each in data:
                re_data = self.__shuffle(each)
                re_list.append({"TABLE_NAME_": self.script_name, "DATA_": re_data})
            return re_list
        else:
            return


if __name__ == '__main__':
    # param = sys.argv[1]

    param = "{'entityType':'CRMMARKETACT','limitNumber':1,'entityCode':['CRMJPFX_YXHD_PFYH']}"
    script = BranchOrganize(table_name=TABLE_NAME("CRM_MARKET_ACT"), collection_name="CRMJPFX_YXHD", param=param)
    script.main()
    script.close_client()

