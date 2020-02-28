# -*- coding: utf-8 -*-
import os
import re
import sys
import time

import requests

from dev_plan_scripts import GenericScript
from tools.req_for_wordExcelZip import transform_data, find_type

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-12])

from tools.req_for_api import req_for_serial_number
from __config import TABLE_NAME, CREATE_ID, CREATE_NAME


class BranchOrganize(GenericScript):

    def __init__(self, table_name, collection_name, param):
        super(BranchOrganize, self).__init__(table_name, collection_name, param, verify_field={"NAME_": "NAME_", "URL_": "URL_"})

    def __shuffle(self, data):
        serial_number = req_for_serial_number(code="JRCP_LCCP_INFO")
        data["ID_"] = serial_number
        content = ''
        data['conten_type'] = find_type(data.get('FJ1_URL_')) if find_type(data.get('FJ1_URL_')) else find_type(data.get('FJ1_NAME_'))
        # 文本分类模型
        try:
            response = requests.post('http://172.22.69.39:8099/ZHclassify', data={'title': data.get('TITLE_')}).json()
        except Exception as e:
            self.logger.exception(f"err: 请求模型 http://172.22.69.39:8099/ZHclassify 错误. {e}")
        else:
            if response:
                data["type"] = response["type"]
            else:
                data["type"] = '发展规划_其他'
        if data.get('CONTENT_'):
            if len(data.get('CONTENT_')) < 500:
                data['accessory'] = str(transform_data(data.get('FJ1_URL_'), data)) if data.get('FJ1_URL_') else ''
            try:
                content = data.get('CONTENT_').replace('|', '') + data.get('accessory') if data.get('FJ1_URL_') else data.get('CONTENT_').replace('|', '')
            except:
                content = data.get('CONTENT_').replace('|', '')
            if content:
                # 文本摘要模型
                try:
                    response = requests.post('http://172.22.69.39:8101/ZHsummary', data={'text': content[:500]}).json()
                except Exception as e:
                    self.logger.exception(f"err: 请求模型 http://172.22.69.39:8099/ZHclassify 错误. {e}")
                    data["summary"] = ''
                else:
                    if response:
                        data["summary"] = response.get("summary")
                    else:
                        data["summary"] = ''

                # 地名及其置信度模型
                try:
                    response = requests.post('http://172.22.69.39:8100/ZHlocation', data={'text': content[:500]}).json()
                except Exception as e:
                    self.logger.exception(f"err: 请求模型 http://172.22.69.39:8099/ZHclassify 错误. {e}")
                    data["location"] = ''
                else:
                    if response:
                        data["location"] = response.get("address")
                    else:
                        data["location"] = ''
        re_data = super(BranchOrganize, self).generic_shuffle(data=data, re_data=data, field="ENTITY_NAME_")
        return re_data

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
    #  GDSZ_SZS_TZJG_XMGS          false
    #  GDSZ_GZS_FGW_FZGH           2
    #  GDSZ_SZS_FGW_GHJH           2
    #  GDSZ_GDS_FGW_FZGH           2
    #  GDSZ_GDS_CJJ_GG             2
    #  GDSZ_GZS_TZCJJ_GKXX         2
    #  GDSZ_GDS_TZJG_XMBLJGGS      2
    #  GDSZ_SWS_FGW_GHJH           2
    #  GDSZ_SGS_FGW_FZGGGZ         2
    #  GDSZ_ZHS_FGW_FZGH           2
    #  GDSZ_SZS_SWJ_TZGG           2
    #  GDSZ_YFS_FGW_GHJH           2  3
    #  GDSZ_FSS_FGW_JHGH           2
    #  GDSZ_ZHS_SWJ_TZGG           2
    #  GDSZ_HZS_FGW_FZGH_BMXGWJ    2
    #  GDSZ_HYS_FGW_XMXX           2
    #  GDSZ_QYS_FGW_ZDLYZL         2
    #  GDSZ_CZS_FGW_ZDXM           2
    #  GDSZ_JYS_FGW_ZDXM           2
    #  GDSZ_FSS_SWJ_TPXW           2
    #  GDSZ_HZS_SWJ_SWZX           2
    #  GDSZ_STS_SWJ_GZDT           2
    #  GZSZ_MZS_SWJ_TZGG           2    3
    #  GDSZ_ZQS_SWJ_GZDT           2    3
    #  GDSZ_MMS_FGW_FZGH_TZGG      2
    #  GDSZ_SGS_SWJ_SWDT           2
    #  GDSZ_MMS_SWJ_SWXW           2    3

    param = "{'entityType':'GOV_ZX_GDS','limitNumber':2000,'entityCode':['GDSZ_SWS_FGW_GHJH']}"
    script = BranchOrganize(table_name=TABLE_NAME("GOV_ZX_GDS"), collection_name="GOV_ZX_GDS", param=param)
    script.main()
    script.close_client()
