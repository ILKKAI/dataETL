# -*- coding: utf-8 -*-
"""ZTB_ZGYZJTGS  中国进出口银行"""
import re
import time
from database._mongodb import MongoClient


def data_shuffle(data):
    if not any([_ in data['TITLE_'] for _ in ['咨询', '培训', '营销']]):
        return None
    # 发布时间清洗
    if "NOTICE_TIME_" in data:
        if data["NOTICE_TIME_"]:
            if isinstance(data["NOTICE_TIME_"], str):
                if ("-" in data["NOTICE_TIME_"]) and ("{" not in data["NOTICE_TIME_"]) and (
                        "[" not in data["NOTICE_TIME_"]):
                    pass
                elif "年" in data["NOTICE_TIME_"] and ("至" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y年%m月%d日")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif "/" in data["NOTICE_TIME_"]:
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y/%m/%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif "\\" in data["NOTICE_TIME_"]:
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y\%m\%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("-" not in data["NOTICE_TIME_"]) and ("\\" not in data["NOTICE_TIME_"]) and (
                        "/" not in data["NOTICE_TIME_"]) and ("年" not in data["NOTICE_TIME_"]) and (
                        "CONTENT" not in data["NOTICE_TIME_"]) and ("公告" not in data["NOTICE_TIME_"]) and (
                        "作者" not in data["NOTICE_TIME_"]) and ("发布" not in data["NOTICE_TIME_"]) and (
                        "工程" not in data["NOTICE_TIME_"]) and ("开始" not in data["NOTICE_TIME_"]):
                    time_array = time.strptime(data["NOTICE_TIME_"], "%Y%m%d")
                    data["NOTICE_TIME_"] = time.strftime("%Y-%m-%d", time_array)

                elif ("{" in data["NOTICE_TIME_"]) or ("[" in data["NOTICE_TIME_"]):
                    time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", data["NOTICE_TIME_"])
                    if time_result:
                        data["NOTICE_TIME_"] = time_result[0]
                else:
                    return None
            else:
                time_result = re.findall(r"20[1-3][0-9]-[01][0-9]-[0-3][0-9]", str(data["NOTICE_TIME_"]))
                if time_result:
                    data["NOTICE_TIME_"] = time_result[0]
                else:
                    data["NOTICE_TIME_"] = ""
    if "NOTICE_TIME_" not in data:
        data["NOTICE_TIME_"] = ""

    data["TITLE_"] = data["TITLE_"].replace("|", "")
    if "CONTENT_" in data:
        data["CONTENT_"] = data["CONTENT_"].replace("|", "")

    return data


def run():
    main_mongo = MongoClient(entity_code="ZTB_ZGYZJTGS", mongo_collection="CommonBidding")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)


def judge(item):

    if not any([num in item for num in ['长城', '长城科技', '长城信息']]):
        return False
    else:
        return True


if __name__ == '__main__':
    # run()

    pass
    import pandas as pd

    main_mongo = MongoClient(entity_code="", mongo_collection="CommonBidding")
    db, collection_list = main_mongo.client_to_mongodb()
    collection = main_mongo.get_check_collection(db=db, collection_list=collection_list)
    # mon_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    mon_list = ['08',
                '09',
                '10',
                '11',
                '12',
                # '06',
                ]
    for _ in range(13):
        try:
            data_list = collection.find({'DATETIME_': {'$gt': f'2019-{mon_list[_-1]}-01', '$lt': f'2019-{mon_list[_]}-01'},
                                         # 'CONTENT_': {'$regex': '/(长城科技|长城信息|长城) /'}
                                         },
                                         {'CONTENT_': 1, 'NOTICE_TIME_': 1, 'TITLE_': 1, 'DATETIME_': 1, 'URL_': 1})

            data = pd.DataFrame(data_list)
            data['CONTENT_'] = data['CONTENT_'].astype(str)
            print(f'处理第{_}项')
            data['judge'] = data['CONTENT_'].apply(judge)
            data_ = data[data['judge']]
            data_all = data_[['NOTICE_TIME_', 'TITLE_', 'URL_']]
            # data_all.drop(columns=['_id', 'CONTENT_', 'DEALTIME_', 'DATETIME_', 'ENTITY_NAME_', 'ENTITY_CODE_'], inplace=True)
            data_all.to_excel(f'C:/Users/xiaozhi/Desktop/长城{mon_list[_]}.xlsx', index=False)
        except Exception as e:
            print(f'第{_}项报错, {e}')
            continue
