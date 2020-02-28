# -*- coding: utf-8 -*-
import re
from io import StringIO

import requests
import urllib3
from lxml.etree import HTML
import os.path
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter, process_pdf
from pdfminer.converter import PDFPageAggregator, TextConverter
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
from PyPDF2 import PdfFileReader, PdfFileWriter
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


text_path = "P020190320544309050551.pdf"


def req_for_file_save(id, file_name, type_code, postfix, file):
    """
    上传文件至 fdfs
    :param id:
    :param file_name:
    :param type_code:
    :param file:
    :return:
    """
    response = requests.post(url="http://172.22.69.41:8095/attachment/fdfs/upload",
                             params={"entityId": id, "attachTypeCode": type_code, "fileName": file_name, "format": postfix},
                             data=file)
    return response


def parse(pdf_url):

    from urllib.request import urlopen
    fp = urlopen(pdf_url)

    # 用文件对象创建一个PDF文档分析器
    parser = PDFParser(fp)
    # 创建一个PDF文档
    doc = PDFDocument()
    # 连接分析器，与文档对象
    parser.set_document(doc)
    doc.set_parser(parser)

    # 提供初始化密码，如果没有密码，就创建一个空的字符串
    doc.initialize()

    # 检测文档是否提供txt转换，不提供就忽略
    if not doc.is_extractable:
        raise PDFTextExtractionNotAllowed
    else:
        # 创建PDF，资源管理器，来共享资源
        rsrcmgr = PDFResourceManager()
        # 创建一个PDF设备对象
        laparams = LAParams()
        device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        # 创建一个PDF解释器对象
        interpreter = PDFPageInterpreter(rsrcmgr, device)

        # 循环遍历列表，每次处理一个page内容
        # doc.get_pages() 获取page列表
        result_list = list()
        for page in doc.get_pages():
            interpreter.process_page(page)
            # 接受该页面的LTPage对象
            layout = device.get_result()
            # 这里layout是一个LTPage对象 里面存放着 这个page解析出的各种对象
            # 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等
            # 想要获取文本就获得对象的text属性，
            text_list = list()
            for x in layout:
                if isinstance(x, LTTextBoxHorizontal):
                    # with open(r'2.txt', 'a', encoding="utf-8") as f:
                    results = x.get_text()
                    text_list.append(results)
            text = "\n".join(text_list)
                    # f.write(results + "\n")
            result_list.append(text)

        return "".join(result_list)


def read_pdf(pdf):
    # resource manager
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    laparams = LAParams()
    # device
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    process_pdf(rsrcmgr, device, pdf)
    device.close()
    content = retstr.getvalue()
    retstr.close()
    # 获取所有行
    lines = str(content).split("\n")

    units = [1, 2, 3, 5, 7, 8, 9, 11, 12, 13]
    header = '\x0cUNIT '
    # print(lines[0:100])
    count = 0
    flag = False
    # text = open('words.txt', 'w+')
    for line in lines:
        if line.startswith(header):
            flag = False
            count += 1
            if count in units:
                flag = True
                print(line)
                # text.writelines(line + '\n')
        # if '//' in line and flag:
        #     text_line = line.split('//')[0].split('. ')[-1]
        #     print(text_line)
        #     text.writelines(text_line + '\n')
    # text.close()


def new_pdf_test(pdf):

    readFile = 'read.pdf'
    writeFile = 'write.pdf'
    # 获取一个 PdfFileReader 对象
    pdfReader = PdfFileReader(open(readFile, 'rb'))
    # 获取 PDF 的页数
    pageCount = pdfReader.getNumPages()
    print(pageCount)
    # 返回一个 PageObject
    page = pdfReader.getPage(1)
    # 获取一个 PdfFileWriter 对象
    pdfWriter = PdfFileWriter()
    # 将一个 PageObject 加入到 PdfFileWriter 中
    pdfWriter.addPage(page)
    # 输出到文件中
    pdfWriter.write(open(writeFile, 'wb'))


if __name__ == '__main__':
    # import certifi
    # http = urllib3.PoolManager(
    #     cert_reqs='CERT_REQUIRED',
    #     ca_certs=certifi.where()
    # )
    # resp = http.request('GET', 'https://per.spdb.com.cn/bank_financing/financial_product/zxlc/201709/P020190404777135897809.pdf')
    # print(resp)
    # result = parse("http://www.iresearch.cn/include/ajax/user_ajax.ashx?work=idown&rid=3362")
    # result = parse("http://www.ccb.com/gd/cn/fhgg/upload//20190412_1555036078/20190412104920557668.pdf")
    # 建设银行 缺少字体
    # result = parse("http://www.ccb.com/gd/cn/fhgg/upload//20190403_1554279393/20190408110307506786.pdf")
    # print(result)
    # result = parse(pdf_url="http://www.ccb.com/gd/cn/fhgg/upload//20190412_1555051954/20190412172311355084.pdf")
    # result = parse(pdf_url="https://per.spdb.com.cn/bank_financing/financial_product/zxlc/201709/P020190404777135897809.pdf")
    # result = parse(pdf_url="http://www.ccb.com/gd/cn/fhgg/upload//20190412_1555036078/20190415153431951068.pdf")
    # print(result)
    # save_to_fdfs()
    # response = requests.get(url="http://ewealth.abchina.com/fs/intro_list/ADZJ195238.htm")
    # headers = {
    #               "Accept": "image/gif, image/jpeg, image/pjpeg, application/x-ms-application, application/xaml+xml, application/x-ms-xbap, */*",
    #               "Accept-Encoding": "gzip,deflate",
    #             "Accept-Language": "zh-Hans-CN, zh-Hans; q=0.5",
    #     # "Cache-Control": "no-cache",
    #     "Connection": "keep-alive",
    #     "DNT": "1",
    #     "Host": "per.spdb.com.cn",
    #     "Pragma": "no-cache",
    #     "Referer": "https://per.spdb.com.cn/bank_financing/financial_product/zxlc/201709/P020190404777135897809.pdf",
    #     "Upgrade-Insecure-Requests": "1",
    #     "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729)"
    # }
    # response = requests.get(url="https://per.spdb.com.cn/bank_financing/financial_product/zxlc/201709/P020190404777135897809.pdf", headers=headers,verify=False)
    # print(response.cookies)
    # print(response.content)
    # print(response.content.decode("utf-8"))
                                 # https://per.spdb.com.cn/bank_financing/financial_product/zxlc/201709/P020190404777135897809.pdf

# chrome-extension://efaidnbmnnnibpcajpcglclefindmkaj/data/js/frame.html?message=%7B%22tabId%22%3A72%2C%22loaded%22%3Afalse%2C%22filename%22%3A%22P020190404777135897809.pdf%22%2C%22panel_op%22%3A%22pdf_menu%22%2C%22url%22%3A%22https%3A%2F%2Fper.spdb.com.cn%2Fbank_financing%2Ffinancial_product%2Fzxlc%2F201709%2FP020190404777135897809.pdf%22%2C%22is_pdf%22%3Atrue%2C%22version%22%3A13%7D

    # 无效 KeyError: 'DescendantFonts'
    # response = requests.get(url="http://www.ccb.com/gd/cn/fhgg/upload//20190403_1554279393/20190408110307506786.pdf")
    # with open("zgjsyh.pdf", "wb") as f:
    #     f.write(response.content)
    # a = open("zgjsyh.pdf", "rb")
    # print(a)
    # read_pdf(a)
    # float('')

    print(parse('http://fgw.gz.gov.cn/gzplan/s15713/201911/736a84a1126f48b19ff162cca06827d5/files/084c2256646e4c08828d8575b3cd01f0.pdf'))

