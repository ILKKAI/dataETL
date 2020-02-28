# -*- coding: utf-8 -*-
"""CRMJPFX_YXHD_SHYH  上海银行-营销活动"""
import jsonpath

from crm_scripts import GenericScript
from database._mongodb import MongoClient
from tools.web_api_of_baidu import get_lat_lng, get_area


def data_shuffle(data, ):
    re_data = dict()
    re_data['ACTIME_NAME_'] = data.get('TITLE_')
    re_data['RELEASE_DATE_'] = data.get('PUBLISH_TIME_')
    re_data['ACTIVE_DESC_HTML_'] = data.get('HTML_')
    re_data['ACTIVE_DESC_TEXT_'] = data.get('CONTENT_')[:501]
    re_data['DATA_SOURCE_NAME_'] = data.get('SOURCE_NAME_')
    re_data['DATA_SOURCE_URL_'] = data.get('URL_')
    re_data['AMOUNT_OF_READING_'] = data.get('READ_NUM_')
    re_data['ACTIVE_KEYWORDS_'] = data.get('')
    re_data['ACTIVE_OBJECT_'] = data.get('')

    re_data['BANK_NAME_'] = data.get('BANK_NAME_')

    return re_data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="CRMJPFX_YXHD_SHYH", mongo_collection="CRMJPFX_YXHD")
    data_list = main_mongo.main()
    for data in data_list[:2]:
        re_data = data_shuffle(data=data, )
        print(re_data)
