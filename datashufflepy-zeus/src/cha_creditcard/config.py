# -*- coding: utf-8 -*-
import time

MONGO_CONFIG = {
    "host": "172.22.69.35",
    # "host": "172.22.69.41",
    "port": 20000,
    # "port": 27017,
    "database": "spider_data",
}

MYSQL_CONFIG = {
    "config": {
        "host": "172.22.69.41",
        "port": 3306,
        "database": "chabei_creditcard",
        "user": "root",
        "password": "dev007%P",
        "charset": "utf8",
    },
}

# 创建时间
CREATE_TIME = lambda: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
# 创建人ID
CREATE_BY_ID = "P0139381"
# 创建人名称
CREATE_BY_NAME = "周郅翔"
