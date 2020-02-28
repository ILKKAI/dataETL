# -*- coding: utf-8 -*-
"""链家-商铺-租赁-成都 WD_JZ_FJ_LISPZL_CD"""


def data_shuffle(data):
    # 商铺名称
    data["TITLE_"] = data["NAME_"]

    return data
