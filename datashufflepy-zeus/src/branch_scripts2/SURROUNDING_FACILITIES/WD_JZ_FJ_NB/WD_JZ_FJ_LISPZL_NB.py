# -*- coding: utf-8 -*-
"""链家-商铺-租赁-宁波 WD_JZ_FJ_LISPZL_NB"""


def data_shuffle(data):
    # 商铺名称
    data["TITLE_"] = data["NAME_"]

    return data
