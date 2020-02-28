# -*- coding: utf-8 -*-
import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath)
# print(curPath)
from log.data_log import Logger

# warnings.filterwarnings("ignore")


# 根据实体code 获取实体类型
class DataShuffle(object):
    def __init__(self, param):
        self.logger = Logger().logger
        self.invoke_type = "BRANCH"
        self.base_dir = os.path.dirname(os.getcwd())
        self.param_dict = eval(param)
        self.param = "\"" + param + "\""
        if self.param_dict:
            try:
                self.entity_type = self.param_dict["entityType"]
            except Exception:
                raise Exception

    def invoking_the_script(self):
        """
        调用脚本
        :return:
        """
        if self.invoke_type == "BRANCH":
            # 微博和微信
            if self.entity_type == "WEIBOINFO" or self.entity_type == "WEIBOBASICINFO" or self.entity_type == "WECHAT":
                file_path = "/".join(["./branch_scripts2", self.entity_type, f"{self.entity_type}.py"])
            # 金融产品
            elif self.entity_type[:4] == "JRCP":
                file_path = "/".join(["./branch_scripts2", "FIN_PRODUCT", self.entity_type, "__init_____.py"])
            # 网点
            elif self.entity_type[:2] == "WD":
                file_path = "/".join(["./branch_scripts2", "SURROUNDING_FACILITIES", self.entity_type, "__init_____.py"])
            # 招投标
            elif self.entity_type == "COMMONBIDDING" or self.entity_type == "commonbidding" or self.entity_type == "CommonBidding":
                file_path = "/".join(["./scripts", "CommonBidding", "__init_____.py"])
            # 招投标
            elif self.entity_type == "ORGANIZE" :
                file_path = "/".join(["./scripts", "CommonBidding", "__init_____.py"])
            else:
                file_path = "/".join(["./crm.scripts", self.entity_type, "__init_____.py"])
        else:
            if self.entity_type == "WEIBOINFO" or self.entity_type == "WEIBOBASICINFO" or self.entity_type == "WECHAT":
                file_path = "/".join(["./branch_scripts2", self.entity_type, f"{self.entity_type}.py"])
            else:
                file_path = "/".join(["./branch_scripts2", self.entity_type, "__init_____.py"])

        self.logger.info("调用的脚本文件为{}".format(file_path))

        os.system(" ".join(["python ", file_path, self.param]))

    def main(self):
        self.invoking_the_script()
        # self.logger.handlers.clear()


if __name__ == '__main__':
    # 参数
    # {
    #     'entityType': 'NEWS, JRCP_BX, CRMJRCP_JJ, JRCP_LCCP, WECHAT, WEIBOINFO, WEIBOBASICINFO',
    #     'entityCode': 'str or list',
    #     'limitNumber': int
    # }
    # param = sys.argv[1]
    # param = "{'entityType': 'spider_data 表名', 'limitNumber': 1, 'entityCode': ['实体名称/查询条件']}"


    # param = "{'entityType': 'WD_TY', 'limitNumber': 1, 'entityCode': ['ABCORGANIZE']}"
    # param = "{'entityType':'WD_JT_GJ','limitNumber':7593,'entityCode':['WD_JT_GJ_GJWZD_NB']}"
    # param = "{'entityType':'WD_SS_XX','limitNumber':7593,'entityCode':['WD_SH_XX_51SXW']}"
    # param = "{'entityType':'WD_TY','limitNumber':20000,'entityCode':['BOCOMORGANIZE1']}"
    # param = "{'entityType':'WD_JZ_FJ_BJ','limitNumber':10,'entityCode':['WD_JZ_FJ_LIXQZL_BJ']}"
    # param = "{'entityType':'JRCP_XYK','limitNumber':2000,'entityCode':['JRCP_XYK_WAK_ALL']}"
    # param = "{'entityType':'JRCP_LCCP','limitNumber':5000,'entityCode':['JRCP_LCCP_ZGJSYH_GW_ALL1']}"
    # param = "{'entityType':'JRCP_LCCP','limitNumber':5000,'entityCode':['JRCP_LCCP_ZGJSYH_GW_ALL1']}"
    # param = "{'entityType':'WD_JT_DT','limitNumber':10,'entityCode':['WD_SH_XX_51SXW']}"
    # param = "{'entityType':'NEWS','limitNumber':1000,'entityCode':['ZX_ZCGG_SJS_ZJHL']}"
    param = "{'entityType':'MAP_BAR','limitNumber':1,'entityCode':['MAPBAR_DEATAIL']}"
    # param = "{'entityType':'ORGANIZE','limitNumber':1,'entityCode':['CRMJPFX_WD_JSYH']}"
    # param = "{'entityType':'COMMONBIDDING','limitNumber':1}"
    # param = "{'entityType':'WECHAT','limitNumber':1}"
    data_shuffle = DataShuffle(param=param)
    data_shuffle.logger.info(f"接收参数为: {param}")
    data_shuffle.main()
