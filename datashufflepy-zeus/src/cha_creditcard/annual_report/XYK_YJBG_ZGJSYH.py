# -*- coding: utf-8 -*-
"""XYK_YJBG_ZGJSYH  同业年报 中国建设银行"""
import re

from database._mongodb import MongoClient
from tools.req_for_pdf import parse


def data_shuffle(data):
    year = re.findall(r"2\d{3}", data["TITLE_"])
    if year:
        data["YEAR_"] = year[0]  # 年份
    else:
        try:
            pdf_content = parse(pdf_url=data["PDF_URL_"])

        except Exception:
            pass
        else:
            year = re.findall(r"(2\d{3}) ?年?年度", pdf_content)
            if year:
                s_year = list(set(year))
                if len(s_year) == 1:
                    data["YEAR_"] = s_year[0]
                else:
                    s_year.sort()
                    data["YEAR_"] = s_year[-1]
            else:
                year = re.findall(r"(2 ?\d ?\d ?\d) 年", pdf_content)
                if year:
                    year.sort()
                    data["YEAR_"] = year[-1].replace(" ", "")
                else:
                    year = re.findall(r"(2 ?\d ?\d ?\d)", pdf_content)
                    if year:
                        year.sort()
                        data["YEAR_"] = year[-1]

    if not data.get("YEAR_"):
        year = re.findall(r"/(2\d{3})/", data["PDF_URL_"])
        if year:
            data["YEAR_"] = year[0]

    data["FILE_URL_"] = data["PDF_URL_"]
    data["FILE_NAME_"] = data["PDF_NAME_"]
    data["TITLE_"] = data["TITLE_"].replace('•', '')
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="XYK_YJBG_ZGJSYH", mongo_collection="XYK_YJBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)
