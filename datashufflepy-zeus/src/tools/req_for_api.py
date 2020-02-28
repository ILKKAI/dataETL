# -*- coding: utf-8 -*-
import uuid
# import jpype
import jaydebeapi
import time
import os, sys
import re

import requests
from retrying import retry

from __config import *
from database._mongodb import MongoClient
from database._mysql import MysqlClient



curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-8])

from log.data_log import Logger



# from database._phoenix_hbase import PhoenixHbase, value_replace


@retry(stop_max_attempt_number=5)
def req_for_serial_number(code):
    """
    生成序列号
    :param code: MySQL 对应 CODE_
    :return:
    """
    response = requests.get(url=f"http://172.22.69.41:8910/serial/next/{code}")
    # response = requests.get(url=f"http://192.168.1.26:8080/serial/next/{code}")
    if not response.text:
        response.close()
        return
    else:
        result = response.json()
        response.close()
        if result["success"]:
            return result["code"]
        else:
            return


@retry(stop_max_attempt_number=5)
def req_for_something(url):
    """
    requests for image, pdf and otherthing
    :param url:
    :return:
    """
    user_agent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                 "Chrome/73.0.3683.103 Safari/537.36"
    headers = {'user-agent': user_agent}
    session = requests.session()
    session.get(url='https://news.spdb.com.cn/investor_relation/periodic_report/', headers=headers, verify=False)
    cookie = {
        'Hm_lvt_e3386c9713baeb4f5230e617a0255dcb':'1570518142,1572077119,1572084188,1572084380' ,
        'Hm_lpvt_e3386c9713baeb4f5230e617a0255dcb':'1572084383' ,
        'TSPD_101':'08e305e14cab280002aff13d9ec9f6c7a340e5a375939dbcbfbbba9ea92cdcc5b74268f575c4041d20e4c19953ece9ba:' ,
        'TS01d02f4c':'01ea722d2af931ad67a44d37f3156a7a2fbf75f1e491e573dbcb12dc89b919e22f1eb1f700883e1c591eaf51fbdec12f5cd4c9be0a'
    }
    # response = session.get(url=url, headers=headers, cookies=cookie, verify=False)
    response = requests.get(url=url, headers=headers, verify=False)
    # if response.status_code == 200:
    if response.content:
        return response
    else:
        response.close()
        return


def req_for_file_save(id, file_name, type_code, postfix, file):
    """
    上传文件至 fdfs
    :param id: HDFS的  row_key
    :param file_name: 文件名称
    :param type_code: CHA_YJBG , 附件类型 CODE
    :param postfix: 数据格式
    :param file: 数据流
    :return:
    """
    response = requests.post(url="http://172.22.69.41:8095/attachment/fdfs/upload",
    # response = requests.post(url="http://172.22.69.40:8095/attachment/fdfs/upload",
    # response = requests.post(url="http://192.168.1.13:8082/attachment/attachment/fdfs/upload",
    # response = requests.post(url="http://192.168.1.38:8080/attachment/fdfs/upload",
                             params={"entityId": id, "attachTypeCode": type_code, "fileName": file_name, "format": postfix},
                             data=file)

    return response


def req_for_file_download(id):
    """
    根据 MySQL 中的 ID_ 下载 fdfs 上的文件
    :param id:
    :return:得到 数据流
    """
    response = requests.post(url=f"http://172.22.69.41:8095/attachment/download/{id}")
    return response


def req_for_file_remove(id):
    """
    根据 MySQL 中的 ID_ 删除 fdfs 上的文件
    :param id:
    :return:
    """
    response = requests.post(url=f"http://172.22.69.41:8095/attachment/remove/{id}")
    return response


def req_for_file_info(entityId, entityType):
    """
    根据 Hbase 中的 ID_ 查询mysql的ID_
    :param entityId:
    :param entityType:
    :return: a map contains attachment id list
    """
    response = requests.post(url="http://172.22.69.41:8095/attachment/queryAttachment", data={"entityId": entityId, "entityType": entityType})
    if response and response.status_code == "200":
        files = response.content
        if len(files) > 0:
            return [attach.id for attach in files]
    return []


if __name__ == '__main__':
    print('成山路支行')
    # 1. 由 hbase 的数据关联到mysql的ID 下载文件到本地,
    # 2. 读取本地文件, 上传到 fdfs
    # table_name = "CHA_ANNUAL_REPORT"
    # test1_ = PhoenixHbase(table_name=table_name)
    # connection = test1_.connect_to_phoenix()
    # save_download_switch = False  # 控制文件的上传/下载
    #
    # if save_download_switch:
    #     # 下载fdfs文件到本地
    #     cur = connection.cursor()
    #     time_array = time.localtime()
    #     create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    #     count = 0
    #     # 返回生成器对象
    #     result_generator = test1_.search_all_from_phoenix(connection=connection, dict_status=True, output_field=None,
    #                                                       where_condition="""  """)
    #     title_list = set()
    #     while True:
    #         try:
    #             data = result_generator.__next__()
    #         except StopIteration:
    #             break
    #         else:
    #             try:
    #                 print(data)
    #                 id = data.get('ID_')
    #
    #                 response = req_for_file_download(id)
    #                 path = f'./FDFS/{data.get("FIEL_NAME_")}___{id}'
    #                 with open(path, 'w+') as fp:
    #                     fp.write(response)
    #             except jaydebeapi.DatabaseError as e:
    #                 print(2, e)
    #                 continue
    #             except Exception as e:
    #                 print(data.get('URL_'), e)
    #             count += 1
    #             print(count)
    #     connection.close()
    # elif not save_download_switch:
    #     # 上传文件
    #     path = './FDFS'
    #     for root, dirs, files in os.walk(path, topdown=False):
    #         for name in files:
    #             file = os.path.join(root, name)
    #             with open(file, 'w+') as fp:
    #                 response = fp.read()
    #             file_name = name.split('___')[0]
    #             id = name.split('___')[-1]
    #             p_response = req_for_file_save(id, file_name, 'CHA_YJBG', 'pdf', response)
    #             if "error" in p_response.content.decode("utf-8"):
    #                  print("上传文件出错")
    #             else:
    #                 print("上传文件成功")
    #             p_response.close()




    # with open("xxx.pdf", "wb") as f:
    #     f.write(response1.content)
    #
    #     # response = requests.get(url="http://file.cmbimg.com/cmbir/20120428/4e280059-5e03-42e8-984e-dd11a5b64a00.pdf")
    #     p_response = req_for_file_save(id=i["v"], type_code="CHA_YJBG",
    #                                    file_name=str(uuid.uuid1()), postfix=i["p"],
    #                                    file=response1.content)
    #     print(p_response.content.decode("utf-8"))
    # import pymysql
    #
    # conn = pymysql.connect(
    #     host='172.22.69.41',
    #     port=3306,
    #     user='root',
    #     password="dev007%P",
    #     database='chabei',
    #     use_unicode=True,
    #     charset="utf8",  # 指定字符编码
    #     autocommit=True,
    #     cursorclass=pymysql.cursors.DictCursor,
    # )
    # id = 'ZX0000019929CJXW'
    # entityType = 'CHA_NEWS_PDF'
    # with conn.cursor() as cursor:
    #     cursor.execute(f'''select ID_ from sys_attachment where ENTITY_ID_ = "{id}" and ATTACH_TYPE_CODE_ = "{entityType}" ''')
    #     mysql_id = cursor.fetchone()
    #     print(mysql_id)
    # print(req_for_serial_number('CRM_MARKET_ACT'))

