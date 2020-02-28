# -*- coding: utf-8 -*-
import re
from cha_creditcard import GenericScript


class AnnalsScript(GenericScript):

    def generic_shuffle(self, data):
        re_data = dict()

        for bank in self.bank_list:
            if data["ENTITY_NAME_"][:-4] in bank["ALIAS_"]:
                re_data["BANK_CODE_"] = bank["CODE_"]  # 银行编码
                re_data["BANK_NAME_"] = bank["NAME_"]  # 银行名称
                break
        if "YEAR_" in data:
            re_data["YEAR_"] = data["YEAR_"]
        else:
            year = re.findall(r"2\d{3}", data["TITLE_"])
            if year:
                re_data["YEAR_"] = year[0]  # 年份

        re_data["TITLE_"] = data["TITLE_"]  # 文件标题
        re_data["FILE_URL_"] = data["FILE_URL_"]  # 文件标题
        re_data["FILE_NAME_"] = data["FILE_NAME_"]  # 文件标题

        re_data["PUSH_DATE_"] = data["NOTICE_TIME_"]  # 发布日期
        re_data["PUBLISH_TIME_"] = data["NOTICE_TIME_"]  # 发布日期
        re_data = super(AnnalsScript, self).generic_shuffle(data, re_data)
        return [{"TABLE_NAME_": self.script_name, "DATA_": re_data}]


if __name__ == '__main__':
    # param = "{}"
    param = "{'entityType':'XYK_YJBG','limitNumber':1000,'entityCode':['XYK_YJBG_ZGJSYH',]}"
    script = AnnalsScript(table_name="CHA_ANNUAL_REPORT_COPY", collection_name="XYK_YJBG",
                          param=param, verify_field={"URL_": "URL_", "TITLE_": 'TITLE_'})
    script.main()
