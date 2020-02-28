# -*- coding: utf-8 -*-
"""CRMJPFX_WD_PFYH  华夏银行-网点"""
from crm_scripts import GenericScript
from database._mongodb import MongoClient
from bs4 import BeautifulSoup
import re
from tools.req_for_api import req_for_something
import base64


def data_shuffle(data):
    re_data = dict()
    re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
    re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
    re_data["URL_"] = data["URL_"]
    # 年费
    re_data["FEE_"] = data["FEE_"]
    # 提现额
    re_data["CASHING_AMOUNT_"] = data["CASHING_AMOUNT_"]
    # 信用额（最高）
    re_data["MOST_AMOUNT_"] = data["MOST_AMOUNT_"]
    # 卡等级
    re_data["CARD_LEVEL_"] = data["CARD_LEVEL_"]
    # 卡组织
    re_data["CARD_ORG_"] = data["CARD_ORG_"]
    # 卡片IMAGE
    if "IMG_" in data:
        image_url = data["IMG_"]
        response = req_for_something(url=image_url)
        if response:
            t = base64.b64encode(response.content)
            re_data["IMG_"] = t.decode("utf-8")
    # 卡片名称
    re_data["CARD_NAME_"] = data["CARD_NAME_"]
    # 权益（文字描述）
    re_data["POWER_WRITING_"] = data["POWER_WRITING_"]
    # 卡属性
    re_data["CARD_ATTR_"] = data["CARD_ATTR_"]
    # 信用额度
    re_data["CREDIT_AMOUNT_"] = data["CREDIT_AMOUNT_"]
    # 免息期
    re_data["INTEREST_FREE_"] = data["INTEREST_FREE_"]
    # 详细介绍
    INTRO_ = BeautifulSoup(data["INTRO_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*卡片介绍([\s\S]*)")
    if re.match(pattern, INTRO_):
        a = re.match(pattern, INTRO_)
        intro = a.group(1)
        intro = re.sub('[\n]+', '', intro)
        re_data["INTRO_"] = intro


    # 卡片介绍
    # print(data["CARD_INTRO_"])
    soup = BeautifulSoup(data["CARD_INTRO_"], "html.parser")
    re_data["CARD_INTRO_"] = soup.find('div', {"class" : "adp"}).text
    # pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")

    # OTHER_REPAY_其他还款
    soup = BeautifulSoup(data["OTHER_REPAY_"], "html.parser")
    # print(soup)
    a = soup.find_all('div', {"class":"tt2_1"})
    OTHER_REPAY_LIST = list()
    for item in a:
        OTHER_REPAY_LIST.append(item.string)
    OTHER_REPAY_ = "|".join(OTHER_REPAY_LIST)
    re_data["OTHER_REPAY_"] = OTHER_REPAY_

    # OFFLINE_REPAY_ 网点还款
    soup = BeautifulSoup(data["OFFLINE_REPAY_"], "html.parser")
    a = soup.find_all('div', {"class":"tt2_1"})
    OFFLINE_REPAY_LIST =list()
    for item in a:
        OFFLINE_REPAY_LIST.append(item.string)
    OFFLINE_REPAY_ = "|".join(OFFLINE_REPAY_LIST)
    re_data["OFFLINE_REPAY_"] = OFFLINE_REPAY_

    # NET_REPAY_ 在线还款
    soup = BeautifulSoup(data["NET_REPAY_"], "html.parser")
    a = soup.find_all('div', {"class": "tt2_1"})
    NET_REPAY_LIST = list()
    for item in a:
        NET_REPAY_LIST.append(item.string)
        NET_REPAY_ = "|".join(NET_REPAY_LIST)
    re_data["NET_REPAY_"] = NET_REPAY_

    # ACTIVATE_ 激活
    re_data["ACTIVATE_"] = data["ACTIVATE_"]

    # SCORE_MILEAGE_ 积分兑换里程
    SCORE_MILEAGE_ = BeautifulSoup(data["SCORE_MILEAGE_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, SCORE_MILEAGE_):
        a = re.match(pattern, SCORE_MILEAGE_)
        score_mileage = a.group(1)
        score_mileage = re.sub('[\n]+', '', score_mileage)
        score_mileage = re.sub('\s+', '', score_mileage)
        re_data["SCORE_MILEAGE_"] = score_mileage

    # SCORE_METHOD_ 积分兑换方法
    SCORE_METHOD_ = BeautifulSoup(data["SCORE_METHOD_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, SCORE_METHOD_):
        a= re.match(pattern, SCORE_METHOD_)
        score_method = a.group(1)
        score_method = re.sub('[\n]+', '', score_method)
        score_method = re.sub('\s+', '', score_method)
        re_data["SCORE_METHOD_"] = score_method

    # SCORE_SEARCH_ 积分查询方式
    SCORE_SEARCH_ = BeautifulSoup(data["SCORE_SEARCH_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, SCORE_SEARCH_):
        a = re.match(pattern, SCORE_SEARCH_)
        score_search = a.group(1)
        score_search = re.sub('[\n]+', '', score_search)
        score_search = re.sub('\s+', '', score_search)
        re_data["SCORE_SEARCH_"] = score_search

    # SCORE_ACCU_ 积分累积规则
    SCORE_ACCU_ = BeautifulSoup(data["SCORE_ACCU_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, SCORE_ACCU_):
        a = re.match(pattern, SCORE_ACCU_)
        score_accu = a.group(1)
        score_accu = re.sub('[\n]+', '', score_accu)
        score_accu = re.sub('\s+', '', score_accu)
        re_data["SCORE_ACCU_"] = score_accu

    # SCORE_VALID_ 积分有效期
    SCORE_VALID_ = BeautifulSoup(data["SCORE_VALID_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, SCORE_VALID_):
        a = re.match(pattern, SCORE_VALID_)
        score_valid = a.group(1)
        score_valid = re.sub('[\n]+', '', score_valid)
        score_valid = re.sub('\s+', '', score_valid)
        re_data["SCORE_VALID_"] = score_valid

    # PREPAYMENT_ 提前还款规定
    PREPAYMENT_ = BeautifulSoup(data["PREPAYMENT_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, PREPAYMENT_):
        a = re.match(pattern, PREPAYMENT_)
        repayment = a.group(1)
        repayment = re.sub('[\n]+', '', repayment)
        repayment = re.sub('\s+', '', repayment)
        re_data["PREPAYMENT_"] = repayment

    # CHARE_DEDUCT_ 手续费扣除方式
    CHARE_DEDUCT_ = BeautifulSoup(data["CHARE_DEDUCT_"], "html.parser").getText()
    pattern = re.compile(r"[\s\S]*内容页\*/[\s\S]*\.link-hover{color:#0066cc; border-bottom:1px dashed #ccc;}([\s\S]*)")
    if re.match(pattern, CHARE_DEDUCT_):
        a = re.match(pattern, CHARE_DEDUCT_)
        chage_deduct = a.group(1)
        chage_deduct = re.sub('[\n]+', '', chage_deduct)
        chage_deduct = re.sub('\s+', '', chage_deduct)
        re_data["CHARE_DEDUCT_"] = chage_deduct

    # NUMBER_RATE_  期数及费率
    # print(data["NUMBER_RATE_"])
    from scrapy.selector import Selector
    import requests
    response = requests.get(data['URL_'], headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'})
    html = Selector(text=response.content.decode('gb2312'))
    trs = html.xpath('//div[@id="fwq1"]//table[@class="MsoNormalTable"]//tr[position()>5 and position()<last()-1]')
    for tr in trs:
        try:
            page = trs.index(tr) + 6
            xpath_ = f'//div[@id="fwq1"]//table[@class="MsoNormalTable"]//tr[{page}]'
            periods_1 = tr.xpath(xpath_ + '/td[1]/p/span[1]/text()').extract()[0]
            rate_1 = tr.xpath(xpath_ + '//td[1]/p/span[2]/text()').extract()[-1]

            periods_2 = tr.xpath(xpath_ + '/td[2]/p/span[1]/text()').extract()[0]
            rate_2 = tr.xpath(xpath_ + '/td[2]/p/span[2]/text()').extract()[-1]
        except:
            periods_1, rate_1, periods_2, rate_2 = '', '', '', '',
        print(periods_1 , rate_1, periods_2, rate_2)
    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="CRMJPFX_XYK_KBB", mongo_collection="CRMJPFX_XYK")
    data_list = main_mongo.main()
    for data in data_list[:1]:
        re_data = data_shuffle(data=data)
        print(re_data)

