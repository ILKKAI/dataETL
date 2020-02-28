# -*- coding: utf-8 -*-
"""CRMJPFX_ZQ_HXZQ 和讯债券"""
from copy import deepcopy

from bs4 import BeautifulSoup

from crm_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data=None, ):
    '''
    处理_id, 爬取时间
    :param data:
    :return: 直接返回数据列表
    '''
    re_data = []

    from scrapy import Selector
    items = Selector(text=data.get('CONTENT_'))
    table_list = items.xpath('//table[@cellspacing="1"]')
    for table in table_list:
        tr_data = {}
        tr_list = table.xpath('//tr[position() > 1]')
        tr_data['NAME_AND_CODE_'] = data.get('NAME_AND_CODE_')
        tr_data['TYPE_'] = data.get('TYPE_')
        tr_data["SPIDER_TIME_"] = data["DATETIME_"]
        tr_data["URL_"] = data["URL_"]
        tr_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        tr_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        # tr_data["DEALTIME_"] = data["DEALTIME_"]
        # tr_data[""] = data["DATETIME_"]
        tr_data['COMPANY_'] = tr_list[3].xpath('td[2]/text()').extract_first()
        tr_data['BONE_CODE_'] = tr_list[4].xpath('td[2]/text()').extract_first()
        tr_data['ABBREVATION_'] = tr_list[5].xpath('td[2]/text()').extract_first()
        tr_data['DELIVERY_TIME_'] = tr_list[6].xpath('td[2]/text()').extract_first()
        tr_data['LIST_DATE_'] = tr_list[7].xpath('td[2]/text()').extract_first()
        tr_data['ISSUANCE_'] = tr_list[8].xpath('td[2]/text()').extract_first()
        tr_data['DENOMINATION_'] = tr_list[9].xpath('td[2]/text()').extract_first()
        tr_data['ISSUE_PRICE_'] = tr_list[10].xpath('td[2]/text()').extract_first()
        tr_data['DEADLINE_'] = tr_list[11].xpath('td[2]/text()').extract_first()
        tr_data['YEAR_RATE_'] = tr_list[12].xpath('td[2]/text()').extract_first()
        tr_data['ADJUSTED_YEAR_RATE_'] = tr_list[13].xpath('td[2]/text()').extract_first()
        tr_data['VALUE_DATE_'] = tr_list[14].xpath('td[2]/text()').extract_first()
        tr_data['DUE_DATE_'] = tr_list[15].xpath('td[2]/text()').extract_first()
        tr_data['CASHING_PRICE_'] = tr_list[16].xpath('td[2]/text()').extract_first()
        tr_data['ISSUE_START_'] = tr_list[17].xpath('td[2]/text()').extract_first()
        tr_data['ISSUE_DEADLINE_'] = tr_list[18].xpath('td[2]/text()').extract_first()
        tr_data['BOND_VALUE_'] = tr_list[19].xpath('td[2]/text()').extract_first()
        tr_data['SUBSCRIBE_OBJECT_'] = tr_list[20].xpath('td[2]/text()').extract_first()
        tr_data['LIST_PLACE_'] = tr_list[21].xpath('td[2]/text()').extract_first()
        tr_data['CREDIT_LEVEL_'] = tr_list[22].xpath('td[2]/text()').extract_first()
        tr_data['DELIVERY_COMPANY_'] = tr_list[23].xpath('td[2]/text()').extract_first()
        tr_data['REPAY_METHOD_'] = tr_list[24].xpath('td[2]/text()').extract_first()
        tr_data['DELIVERY_GUARANTOR_'] = tr_list[25].xpath('td[2]/text()').extract_first()
        tr_data['DELIVERY_METHOD_'] = tr_list[26].xpath('td[2]/text()').extract_first()
        tr_data['DELIVERY_OBJECT_'] = tr_list[27].xpath('td[2]/text()').extract_first()
        tr_data['UNDERWRITER_INSTITUTION_'] = tr_list[28].xpath('td[2]/text()').extract_first()
        tr_data['TAX_STATUS_'] = tr_list[29].xpath('td[2]/text()').extract_first()
        tr_data['BOND_TYPE_'] = tr_list[30].xpath('td[2]/text()').extract_first()
        tr_data['REMARK_'] = tr_list[31].xpath('td[2]/text()').extract_first()

        re_data.append(tr_data)

    return re_data


if __name__ == '__main__':
    # re_data = data_shuffle()
    main_mongo = MongoClient(entity_code="CRMJPFX_ZQ_HXZQ", mongo_collection="CRMJPFX_ZQ")
    data_list = main_mongo.main()
    for data in data_list:
        if data.get('URL_') == 'http://bond.money.hexun.com/all_bond/020014_gz.shtml':
            re_data = data_shuffle(data=data,)
            print(re_data)
