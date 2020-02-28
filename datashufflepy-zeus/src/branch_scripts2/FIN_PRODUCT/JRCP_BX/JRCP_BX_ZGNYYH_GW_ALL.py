# -*- coding: utf-8 -*-
import re

from database._mongodb import MongoClient
from tools.req_for_api import req_for_something


def data_shuffle(data):
    if "PDF_1_" in data.keys():
        for i in range(10):
            try:
                if ".HTM" in data[f"PDF_{i}_"] or ".htm" in data[f"PDF_{i}_"]:
                    response = req_for_something(url=data[f"PDF_{i}_"])
                    if response:
                        profix_url = re.findall(r"https?://.*/", data[f"PDF_{i}_"])[0]
                        pdf_url = re.findall(r"/\w+\.pdf", response.content.decode("utf-8"))
                        if pdf_url:
                            data[f"PDF_{i}_"] = profix_url[:-1] + pdf_url[0]
            except Exception as e:
                continue

    # data["IMAGES_"] = data["PRO_DETAIL_"]
    # del data["PRO_DETAIL_"]
    return data
    # try:
    #     if "PDF_" in data:
    #         f_pdf = re.findall(r"href=\"./(.*?)\"", data["PDF_"])
    #         f_name = re.findall(r"([\u4e00-\u9fa5]+)</a>", data["PDF_"])
    #         if f_pdf:
    #             for each in f_pdf:
    #                 url = "http://ewealth.abchina.com/Insurance/" + each
    #                 response2 = req_for_something(url=url)
    #                 # print(response2.content.decode("utf-8"))
    #
    #                 pdf_url1 = re.findall(r"/\w+\.pdf", response2.content.decode("utf-8"))
    #                 if pdf_url1:
    #                     pdf_url = "http://ewealth.abchina.com/Insurance/" + pdf_url1[0]
    #                     # data["PDF_"] = ""
    #                     data[f"PDF_{f_pdf.index(each)}"] = pdf_url
    #         del data["PDF_"]
    # except Exception as e:
    #     pass
    # finally:


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_ZGNYYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        # print(re_data)
