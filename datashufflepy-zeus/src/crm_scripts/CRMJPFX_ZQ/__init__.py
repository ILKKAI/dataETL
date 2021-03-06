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
        super(BranchOrganize, self).__init__(table_name, collection_name, param, verify_field={})

    def __shuffle(self, data):
        serial_number = req_for_serial_number(code="CRM_ZQ")
        data["ID_"] = serial_number

        # 创建时间及操作人
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        data["CREATE_TIME_"] = create_time
        data["CREATE_BY_ID_"] = CREATE_ID
        data["CREATE_BY_NAME_"] = CREATE_NAME
        data["M_STATUS_"] = "N"
        data["DELETE_STATUS_"] = "N"
        data["DATA_STATUS_"] = "UNCHECK"
        data["PUBLISH_STATUS_"] = "N"
        data["HOT_"] = "0"
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        if source:
            data["SOURCE_"] = source[0]
        data["SOURCE_NAME_"] = data["ENTITY_NAME_"]

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
    # 清洗数据会先检验 verify_field 字段, 然后依照 verify_field 字段查询 hbase 去重查询
    # 债券数据未做去重处理, 所有不能重复插入
    # param = sys.argv[1]
    param = "{'entityType':'CRMJPFX_ZQ','limitNumber':10000,'entityCode':['CRMJPFX_ZQ_HXZQ']}"
    script = BranchOrganize(table_name=TABLE_NAME("CRMZQ"), collection_name="CRMJPFX_ZQ", param=param)
    script.main()
    script.close_client()
