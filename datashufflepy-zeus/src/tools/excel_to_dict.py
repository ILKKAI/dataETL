# -*- coding: utf-8 -*-
import os
curPath = os.path.abspath(os.path.dirname(__file__))
import xlrd


def excel_to_dict(Path):
    # 打开文件
    workbook = xlrd.open_workbook(Path)

    # 获取所有sheet
    # print(workbook.sheet_names())  # [u'sheet1', u'sheet2']
    # return workbook
    sheet_name = workbook.sheet_names()[0]

    # 根据sheet索引或者名称获取sheet内容
    # sheet = workbook.sheet_by_index(0)  # sheet索引从0开始
    sheet = workbook.sheet_by_name(sheet_name)

    # sheet的名称，行数，列数
    # print(sheet.name, sheet.nrows, sheet.ncols)
    cols1 = sheet.col_values(0)
    data_list = list()
    data_dict = dict()
    for index in range(len(cols1)):
        if index == 0:
            key_list = [key for key in sheet.row_values(index)]
            # print(key_list)
        else:
            key_value = [value for value in sheet.row_values(index)]
            # print(key_value)
            # # for i in range(len(key_list)):
            temp_dict = dict()
            for i in range(len(key_list)):
                temp_dict[key_list[i]] = key_value[i]
                # print(temp_dict)
            data_list.append(temp_dict)
    return data_list


# print(colsOfOrigin)

# # 获取单元格内容
# print
# sheet2.cell(1, 0).value.encode('utf-8')
# print(sheet.cell(0,0).value.encode('utf-8'))
# print
# sheet2.row(1)[0].value.encode('utf-8')
#
# # 获取单元格内容的数据类型
# print
# sheet2.cell(1, 0).ctype

if __name__ == '__main__':
    print(curPath)
    excel_to_dict()
