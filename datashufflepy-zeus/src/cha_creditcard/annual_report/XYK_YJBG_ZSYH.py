# -*- coding: utf-8 -*-
"""XYK_YJBG_ZSYH  同业年报 招商银行"""
import re

from database._mongodb import MongoClient
from tools.req_for_pdf import parse

WORD_DICT = {"一": "01", "二": "02", "三": "03", "四": "04", "五": "05", "六": "06", "七": "07", "八": "08", "九": "09",
             "十": "10", "十一": "11", "十二": "12", "十三": "13", "十四": "14", "十五": "15", "十六": "16", "十七": "17",
             "十八": "18", "十九": "19", "二十": "20", "二十一": "21", "二十二": "22", "二十三": "23", "二十四": "24",
             "二十五": "25", "二十六": "26", "二十七": "27", "二十八": "28", "二十九": "29", "三十": "30", "三十一": "31"
             }
YEAR_DICT = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5", "六": "6", "七": "7", "八": "8", "九": "9", "〇": "0"}


def data_shuffle(data):
    data["FILE_URL_"] = data["URL_"]
    data["FILE_NAME_"] = data["TITLE_"][:-2]
    try:
        pdf_content = parse(pdf_url=data["URL_"])
    except Exception:
        publish_date = re.findall(r"cmbir/(\d{8})/", data["URL_"])
        if publish_date:
            data["NOTICE_TIME_"] = "-".join([publish_date[0][:4], publish_date[0][4:6], publish_date[0][6:]])
    else:
        publish_date = re.findall(r"[〇一二三四五六七八九十]{4} ?年[〇一二三四五六七八九十]{1,2} ?月[〇一二三四五六七八九十]{1,3} ?日", pdf_content)
        if publish_date:
            s_publish_date = list(set(publish_date))
            each_date = s_publish_date[-1].replace(" ", "")
            d_year = each_date[:4]
            d_month = each_date[each_date.find("年") + 1:each_date.find("月")]
            d_day = each_date[each_date.find("月") + 1:each_date.find("日")]
            f_year = "".join([YEAR_DICT[i] for i in d_year])
            data["NOTICE_TIME_"] = "-".join([f_year, WORD_DICT[d_month], WORD_DICT[d_day]])
        else:
            publish_date = re.findall(r"cmbir/(\d{8})/", data["URL_"])
            if publish_date:
                data["NOTICE_TIME_"] = "-".join([publish_date[0][:4], publish_date[0][4:6], publish_date[0][6:]])
    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="XYK_YJBG_ZSYH", mongo_collection="XYK_YJBG")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data["URL_"])
        print(re_data["NOTICE_TIME_"])
