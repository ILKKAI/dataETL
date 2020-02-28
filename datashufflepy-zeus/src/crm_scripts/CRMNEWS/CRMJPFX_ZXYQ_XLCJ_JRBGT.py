# -*- coding: utf-8 -*-
"""CRMJPFX_ZXYQ_XLCJ_JRBGT  新浪财经-金融投诉"""
import jsonpath

from crm_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, ):
    re_data = dict()
    re_data['NEWS_TITLE_'] = data.get('TITLE_')
    re_data['NEWS_TIME_'] = data.get('PUBLISH_TIME_')
    re_data['NEWS_DESC_HTML_'] = data.get('HTML_')
    re_data['NEWS_DESC_TEXT_'] = data.get('CONTENT_')[:501]
    re_data['NEWS_TYPE_'] = data.get('NEWS_TYPE_')
    re_data['DATAS_SOURCE_URL_'] = data.get('URL_')
    re_data['AMOUNT_OF_READING_'] = data.get('READ_NUM_')
    re_data['NEWS_AUTHOR_'] = data.get('AUTHOR_')
    re_data['NEWS_DATA_RESOURCES'] = data.get('SOURCE_NAME_')

    re_data['NEWS_KEYWORDS'] = data.get('')

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="CRMJPFX_ZXYQ_XLCJ_JRBGT", mongo_collection="CRMJPFX_ZXYQ")
    data_list = main_mongo.main()
    for data in data_list[:2]:
        re_data = data_shuffle(data=data, )
        print(re_data)
