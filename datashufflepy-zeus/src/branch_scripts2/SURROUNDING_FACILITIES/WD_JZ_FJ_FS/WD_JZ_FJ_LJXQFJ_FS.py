# -*- coding: utf-8 -*-
"""链家-小区-小区房价-佛山 WD_JZ_FJ_LJXQFJ_FS"""
from copy import deepcopy


def data_shuffle(data):
    data["TITLE_"] = data["NAME_"]
    re_data = deepcopy(data)
    del re_data['_id']
    return re_data

