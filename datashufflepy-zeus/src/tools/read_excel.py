# -*- coding: utf-8 -*-
import xlrd

from tools.req_for_api import req_for_something


def read_excel(text):
    # 打开文件
    workbook = xlrd.open_workbook(file_contents=text, formatting_info=True)

    # 获取所有sheet
    # print(workbook.sheet_names())  # [u'sheet1', u'sheet2']
    return workbook
    sheet_name = workbook.sheet_names()[0]

    # 根据sheet索引或者名称获取sheet内容
    # sheet = workbook.sheet_by_index(0)  # sheet索引从0开始
    sheet = workbook.sheet_by_name(sheet_name)

    # sheet的名称，行数，列数
    print(sheet.name, sheet.nrows, sheet.ncols)

    # 获取合并单元格

    print(sheet.merged_cells)
    print(len(sheet.merged_cells))
    # # 获取整行和整列的值（数组）
    # rows1 = sheet.row_values(3)  # 获取第四行内容
    # rows2 = sheet.row_values(4)  # 获取第四行内容
    # rows3 = sheet.row_values(5)  # 获取第四行内容
    # cols = sheet.col_values(2)  # 获取第三列内容
    # print(rows1)
    # print(rows2)
    # print(rows3)

    # # 获取单元格内容
    # print
    # sheet2.cell(1, 0).value.encode('utf-8')
    # print
    # sheet2.cell_value(1, 0).encode('utf-8')
    # print
    # sheet2.row(1)[0].value.encode('utf-8')
    #
    # # 获取单元格内容的数据类型
    # print
    # sheet2.cell(1, 0).ctype


if __name__ == '__main__':
    response = req_for_something(url="http://www.hxb.com.cn/images/grjr/zjyw/dlxsl/2018/10/12/12165251E4AD1CEEB164E48700BC924FC58F1BED.xls")
    read_excel(response.content)


