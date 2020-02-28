# -*- coding: utf-8 -*-
"""配置文件"""

"MongoDB 默认配置"
# MongoDB 地址
MONGO_HOST_35 = ["172.22.69.35:20000"]
MONGO_HOST_LH = ["localhost:27017"]
MONGO_HOST_25 = ["172.22.67.25:27017"]
MONGO_HOST_41 = ["172.22.69.41:27017"]
MONGO_PRO = ["192.168.1.73:27017", "192.168.1.81:27017", "192.168.1.102:27017"]
# 电信mongo


# MongoDB 数据库(str)
MONGO_DB = "spider_data"
# MONGO_DB = "spider_data_old"

# HBase 默认配置
# HBase 地址(str)
HBASE_HOST = "172.22.69.36"
# thrift 端口(int)
HBASE_PORT = 9090
# HBase 表名(str)
# HBASE_TABLE = "spider_data_test3"
# HBase 列族名(str)
HBASE_COLUM_FAMILY = "C"

"MySQL 线上配置"
# MySQL 地址(str)
MYSQL_HOST = "172.22.69.43"
# MySQL 端口(int)
MYSQL_PORT = 3306
# MySQL 数据库(str)
MYSQL_DATABASE = "chabei"
# # MySQL 表名(str)
MYSQL_TABLE = "cha_di_position"
# MySQL 用户名
MYSQL_USER = "chabei"
# MySQL 密码
MYSQL_PASSWORD = "chabei#O2018"

"MySQL 43"
# MySQL 地址(str)
MYSQL_HOST_43 = "172.22.69.43"
# MySQL 端口(int)
MYSQL_PORT_43 = 3306
# MySQL 数据库(str)
MYSQL_DATABASE_43 = "spider"
# MySQL 表名(str)
MYSQL_TABLE_43 = "spi_scra_entity"
# MySQL 用户名
MYSQL_USER_43 = "spider"
# MySQL 密码
MYSQL_PASSWORD_43 = "spider#O2018"

# MySQL 地址(str)
MYSQL_HOST_25 = "172.22.67.25"
MYSQL_HOST_41 = "172.22.69.41"
# MySQL 端口(int)
MYSQL_PORT_25 = 3306
MYSQL_PORT_41 = 3306
# MySQL 数据库(str)
MYSQL_DATABASE_25 = "chabei"
MYSQL_DATABASE_41 = "chabei"
# MySQL 表名(str)
MYSQL_TABLE_25 = "sys_dict_item"
MYSQL_TABLE_41 = "sys_dict_item"
# MySQL 用户名
MYSQL_USER_25 = "root"
MYSQL_USER_41 = "root"
# MySQL 密码
MYSQL_PASSWORD_25 = "Code123!@#"
MYSQL_PASSWORD_41 = "dev007%P"

# HBase 数据插入者
CREATE_ID = "P0139381"
CREATE_NAME = "周郅翔"

# AI 模型路径
AI_PATH = "C:/Users/kevin/Desktop/ai-training"

# AI 版本号
# 命名实体识别模型
NER_VERSION = "1.0"
# 微博评论情感分类m模型
SINACOMMENT_VERSION = "1.0"
# 新闻资讯正负面识别模型
SENTIMENT_VERSION = "1.0"
# 文本摘要模型
TEXTRANK_VERSION = "1.0"
# 资讯敏感度识别(文本审核)模型
CENSOR_VERSION = "1.0"
# 微博热度等级计算模型
WEIBOHOT_VERSION = "1.0"
# 资讯最大概率地址模型
TEXTLOC_VERSION = "1.0"
# poi机构名去重模型
LOCDUPLICATE_VERSION = "1.0"
# 资讯热度值分级计算模型
NEWSHOT_VERSION = "1.0"
# 财资（财资和本地）相关性模型
TEXTSIMI_VERSION = "1.0"
# 基金产品相关性模型
FUNDPRO_VRESION = "1.0"
# 理财产品相关性模型
FINPRO_VERSION = "1.0"

# 环境
ENV = "pro"  # 生产环境
# ENV = "dev"  # 测试环境

TABLE_NAME = lambda name: name if ENV == "pro" else "TEST_" + name
