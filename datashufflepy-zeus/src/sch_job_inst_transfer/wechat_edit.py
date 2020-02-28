# -*- coding: utf-8 -*-
import os
import re
import sys
import uuid

import requests

curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-22])
# print(curPath[:-22])


from database._mysql import MysqlClient

TYPE_CODE_ = "CHA_WECHAT_HTML"
POST_FIX = "html"
CONFIG = {
    # "host": "192.168.1.103",
    "host": "172.22.69.41",
    "port": 3306,
    "database": "chabei",
    # "user": "chabei",
    "user": "root",
    # "password": "chabei#O2018",
    "password": "dev007%P",
    "table": "sys_attachment"
}
CLIENT = MysqlClient(**CONFIG)
CONNECTION = CLIENT.client_to_mysql()


def req_for_file_save(id, file_name, type_code, postfix, file):
    """
    上传文件至 fdfs
    :param id:
    :param file_name:
    :param type_code:
    :param file:
    :return:
    """
    # response = requests.post(url="http://172.22.69.41:8095/attachment/fdfs/upload",
    response = requests.post(url="http://172.22.69.40:8095/attachment/fdfs/upload",
                             params={"entityId": id, "attachTypeCode": type_code, "fileName": file_name,
                                     "format": postfix},
                             data=file)

    return response


def req_for_file_download(id):
    response = requests.post(url=f"http://172.22.69.40:8095/attachment/download/{id}")
    # response = requests.post(url=f"http://192.168.1.38:8080/attachment/download/{id}")
    result = response.content
    response.close()
    return result


def req_for_file_remove(id):
    response = requests.post(url=f"http://172.22.69.40:8095/attachment/remove/{id}")
    result = response.content
    response.close()
    return result


def get_data_from_mysql():
    result = CLIENT.search_from_mysql(connection=CONNECTION,
                                      where_condition=f"WHERE ATTACH_TYPE_CODE_ = '{TYPE_CODE_}'")
    return result


if __name__ == '__main__':
    response = req_for_file_download(id="61108039-b2db-11e9-9a54-000c29722e96")
    with open("xxx.html", "wb") as f:
        f.write(response)

    # result = get_data_from_mysql()
    # for index, each in enumerate(result):
    #     # print(each["ID_"])
    #     content = req_for_file_download(id=each["ID_"])
    #
        # re_content = re.sub(r"[\n\t\r]+", "", content.decode("utf-8"))
        # re_content = re.sub(r"<script.*?</script>", "", re_content)
        # re_content = re.sub(r"href=\".*?\"", "href=\"javascript:void(0);\"", re_content)
    #
    #     req_for_file_remove(id=each["ID_"])
    #
    #     CLIENT.delete_from_mysql(connection=CONNECTION, where_condition=f"WHERE ID_ = \'{each['ID_']}\'")
    #
    #     new_name = str(uuid.uuid1())
    #     # print(each["ENTITY_ID_"])
    #     # print(new_name)
    #     result = req_for_file_save(id=each["ENTITY_ID_"], file_name=new_name,
    #                                type_code=TYPE_CODE_, postfix=POST_FIX, file=re_content.encode("utf-8"))
    #     print(index, new_name, result.text)
    # CONNECTION.close()
