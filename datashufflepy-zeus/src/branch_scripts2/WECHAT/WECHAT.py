# -*- coding: utf-8 -*-
import base64
import os
import re
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-15])
from __config import TABLE_NAME
from branch_scripts2 import GenericScript
from tools.req_for_api import req_for_serial_number
from tools.excel_to_dict import excel_to_dict
from log.data_log import Logger
# print(curPath)


class WechatScript(GenericScript):

    def __init__(self, table_name, collection_name, param, verify_field):

        super(WechatScript, self).__init__(table_name, collection_name, param, verify_field=verify_field)
        # self.dict1 = dict()
        # with open('./wechat.txt', 'r', encoding='UTF-8-sig')as f:
        #     for line in f:
        #         data = line.replace("\n", "")
        #         self.dict1[data.split(',')[1]] = data.split(',')[0]
        self.excel_dict = excel_to_dict(curPath + "/wechat.xlsx")

    def generic_shuffle(self, data):
        re_data = dict()
        if data["TITLE_"]:
            serial_number = req_for_serial_number(code="WECHAT")
            re_data["ID_"] = serial_number

            re_data["PERIOD_CODE_"] = data["PERIOD_CODE_"].replace("-", "")

            # re_data["SOURCE_"] = data[""]
            # re_data["SOURCE_NAME_"] = data[""]

            re_data["SOURCE_TYPE_"] = "WECHAT"
            re_data["HOT_"] = "0"
            re_data["PUBLISH_TIME_"] = data["PERIOD_CODE_"]
            # .replace("&quot;", "").replace("&amp;", "")
            re_data["TITLE_"] = data["TITLE_"]
            t = base64.b64encode(re_data["TITLE_"].encode("utf-8"))
            re_data["TITLE_CODE_"] = t.decode("utf-8")
            re_data["WECHAT_ID_"] = data["WECHAT_"].strip()
            for wechat_item in self.excel_dict:
                if re_data["WECHAT_ID_"] == wechat_item["WECHAT_CODE_"]:
                    re_data["WECHAT_NAME_"] = wechat_item["WECHAT_NAME_"]
                    re_data["PROVINCE_NAME_"] = wechat_item["PROVINCE_NAME_"]
                    re_data["PROVINCE_CODE_"] = str(wechat_item["PROVINCE_CODE_"])
                    if "." in re_data["PROVINCE_CODE_"]:
                        re_data["PROVINCE_CODE_"] = re_data["PROVINCE_CODE_"].split(".")[0]
                    re_data["CITY_NAME_"] = wechat_item["CITY_NAME_"]
                    re_data["CITY_CODE_"] = str(wechat_item["CITY_CODE_"])
                    if "." in re_data["CITY_CODE_"]:
                        re_data["CITY_CODE_"] = re_data["CITY_CODE_"].split(".")[0]
                    re_data["LAT_"] = str(wechat_item["LAT_"])
                    re_data["LNG_"] = str(wechat_item["LNG_"])
                    break

            re_data["IMPORTANCE_"] = "N"
            re_data["READS_"] = "0"
            re_data["COMMENTS_"] = "0"
            # re_data["ACT_"] = data[""]
            # re_data["ACT_TYPE_"] = data[""]
            # 补录
            # re_data["TYPE_"] = data[""]
            # re_data["TYPE_CODE_"] = data[""]

            re_data["PUBLISH_STATUS_"] = "N"
            re_data["SENSITIVE_"] = "N"
            # # 模型
            # censor = req_for_censor("".join(re.findall(r"\w+", data["CONTENT_"])))
            # if censor:
            #     if censor["censor"] == "N":
            #         re_data["SENSITIVE_"] = "N"
            #     else:
            #         re_data["SENSITIVE_"] = "Y"
            #         re_data["SENSITIVE_WORD_"] = censor["words"]

            re_data["VERSION_"] = "0"
            re_data["RECOMMEND_"] = "0"

            html = re.sub(r"[\n\t\r]+", "", data["CONTENT_"])
            html = re.sub(r"<script.*?</script>", "", html)
            html = re.sub(r"href=\".*?\"", "href=\"javascript:void(0);\"", html)

            del data["CONTENT_"]
            data["HTML_"] = html

            re_data = super(WechatScript, self).generic_shuffle(data=data, re_data=re_data, field="ENTITY_NAME_")
            if re_data.get('_id'):
                del re_data['_id']
            return [{"TABLE_NAME_": self.p_client.table_name, "DATA_": re_data}]
        else:
            return


if __name__ == '__main__':
    # param = sys.argv[1]
    # verify_field = {'TITLE_': 'TITLE_', 'WECHAT_ID_': 'WECHAT_ID_'}
    verify_field = {'TITLE_': 'TITLE_'}
    # param = "{}"
    param = "{'limitNumber':'2'}"
    script = WechatScript(table_name=TABLE_NAME("CHA_BRANCH_WECHAT"), collection_name="WECHAT", param=param, verify_field=verify_field)
    script.main()
    script.close_client()
