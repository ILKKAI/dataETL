# -*- coding: utf-8 -*-
from database._mongodb import MongoClient
from tools.req_for_api import req_for_something
from tools.read_excel import read_excel


def data_shuffle(data):
    if data.get("excel"):
        data_list = []
        response = req_for_something(url=data["excel"])
        work_book = read_excel(response.content)
        sheet_name = work_book.sheet_names()[0]
        sheet = work_book.sheet_by_name(sheet_name)
        com_name_ = ""
        row_list = sheet.row_values(2)
        for n in range(3, sheet.nrows):
            data_item = {}
            for k,v in data.items():
                data_item[k] = v
            rows1 = sheet.row_values(n)
            sheet_dict = dict(zip(row_list, rows1))
            if sheet_dict["保险公司"]:
                com_name_ = sheet_dict["保险公司"]
            else:
                sheet_dict["保险公司"] = com_name_
            data_item["COM_NAME_"] = sheet_dict["保险公司"]
            data_item["PRO_NAME_"] = sheet_dict["保险产品名称"]
            data_item["ENSURE_SOURCE_TYPE_"] = sheet_dict["产品类型"]
            data_list.append(data_item)
    # return data_list


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_BX_HXYH_GW_ALL", mongo_collection="JRCP_BX")
    data_list = main_mongo.main()
    for data in data_list:
        data_list = data_shuffle(data)
        for item in data_list:
            print(item)
