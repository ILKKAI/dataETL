# -*- coding: utf-8 -*-
"""ZTB_ZGYZJTGS  中国进出口银行"""
import re
import time
from database._mongodb import MongoClient


def judge(item):
    # if not any([num in item for num in ['供应链', '产业链', '应收账款', '应收款']]):
    if not any([num in item for num in ['项目','公告']]):
        return False
    else:
        # if not any([num in item for num in ['微信', '营销', 'APP', 'app', 'App']]):
        #     return False
        # else:
        #     return True
        return True


if __name__ == '__main__':

    import pandas as pd

    main_mongo = MongoClient(entity_code="", mongo_collection="CommonBidding")
    db, collection_list = main_mongo.client_to_mongodb()
    collection = main_mongo.get_check_collection(db=db, collection_list=collection_list)
    # mon_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    # mon_list = ['12', '01']
    # mon_list = ['08', '09', '10', '11', '12']


    try:
        data_list = collection.find({'$and': [{'NOTICE_TIME_': {'$gte': '2019-01-01'}},
                                               {'NOTICE_TIME_': {'$lt': '2019-12-31'}},
                                              ]
                                     },
                                     {'NOTICE_TIME_': 1, 'TITLE_': 1, 'DATETIME_': 1, 'URL_': 1,
                                      'CONTENT_': 1,
                                      })
        data = pd.DataFrame(data_list)
        print(len(data))

        data['TITLE_'] = data['TITLE_'].astype(str)
        data['judge'] = data['TITLE_'].apply(judge)
        # data['CONTENT_'] = data['CONTENT_'].astype(str)
        # data['judge'] = data['CONTENT_'].apply(judge)

        data_ = data[data['judge']]
        data_all = data_[['NOTICE_TIME_', 'TITLE_', 'URL_']]
        print(len(data_all))
        data_all.to_excel(f'C:/Users/lyial/Desktop/TF_data.xlsx', index=False)
        print('完成')
    except Exception as e:
        print('error')

