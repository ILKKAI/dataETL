# -*- coding: utf-8 -*-
import base64
import os
import re
import sys
import time

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath[:-12])

from tools.req_for_api import req_for_serial_number, req_for_something
from crm_scripts import GenericScript
from __config import TABLE_NAME, CREATE_ID, CREATE_NAME


class BranchOrganize(GenericScript):

    def __init__(self, table_name, collection_name, param):
        super(BranchOrganize, self).__init__(table_name, collection_name, param,
                                             verify_field={"NAME_": "NAME_", "URL_": "URL_"})
        # self.name_dict = {'工行': 'ICBC', '工商银行': 'ICBC', '农行': 'ABC', '农业银行': 'ABC', '中行': 'BOC', '中银': 'BOC',
        #                   '中国银行': "BOC",
        #                   '建行': 'CCB', '建信': 'CCB', '建设银行': 'CCB', '交行': 'BCM', '交通银行': 'BCM', '邮储银行': 'PSBC',
        #                   '浙商银行': 'CZB',
        #                   '渤海银行': 'CBHB', '中信银行': 'ECITIC', '光大银行': 'CEB', '华夏银行': 'HB', '招行': 'CMB', '招商银行': 'CMB',
        #                   '兴业银行': 'CIB', '广发银行': 'CGB', '平安银行': 'PAB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL',
        #                   '浦东发展银行': 'SPDB', '民生银行': 'CMBC', '汇丰银行': 'HSBC', '渣打银行': 'SC'}

        self.name_dict = {'中国邮政储蓄银行': 'PSBC', '工商银行': 'ICBC', '农业银行': 'ABC', '中国银行': 'BOC', '建设银行': 'CCB',
                          '交通银行': 'COMM', '中信银行': 'CITIC',
                          '光大银行': 'CEB', '华夏银行': 'HXBANK', '民生银行': 'CMBC', '广发银行': 'GDB', '浦东发展银行': 'SPDB',
                          '招商银行': 'CMB', '兴业银行': 'CIB',
                          '恒丰银行': 'EGBANK', '浙商银行': 'CZBANK', '渤海银行': 'BOHAIB', '平安银行': 'SPABANK', '企业银行': 'DIYEBANK',
                          '上海银行': 'SHBANK',
                          '厦门银行': 'XMBANK', '北京银行': 'BJBANK', '福建海峡银行': 'FJHXBC', '吉林银行': 'JLBANK', '宁波银行': 'NBBANK',
                          '温州银行': 'WZCB',
                          '广州银行': 'GCB', '汉口银行': 'HKB', '洛阳银行': 'LYB', '大连银行': 'DLB', '河北银行': 'BHB', '杭州商业银行': 'HZCB',
                          '南京银行': 'NJCB',
                          '乌鲁木齐市商业银行': 'URMQCCB', '绍兴银行': 'SXCB', '葫芦岛市商业银行': 'HLDCCB', '郑州银行': 'ZZBANK',
                          '宁夏银行': 'NXBANK', '齐商银行': 'QSBANK',
                          '锦州银行': 'BOJZ', '徽商银行': 'HSBANK', '重庆银行': 'CQBANK', '哈尔滨银行': 'HRBANK', '贵阳银行': 'GYCB',
                          '兰州银行': 'LZYH', '南昌银行': 'NCB',
                          '青岛银行': 'QDCCB', '青海银行': 'BOQH', '台州银行': 'TZCB', '长沙银行': 'CSCB', '赣州银行': 'GZB',
                          '内蒙古银行': 'H3CB', '包商银行': 'BSB',
                          '龙江银行': 'DAQINGB', '上海农商银行': 'SHRCB', '深圳农村商业银行': 'SRCB', '广州农村商业银行': 'GZRCU',
                          '东莞农村商业银行': 'DRCBCL', '北京农村商业银行': 'BJRCB',
                          '天津农村商业银行': 'TRCB', '江苏省农村信用社联合社': 'JSRCU', '浙江泰隆商业银行': 'ZJQL'}

        # self.level_dict = {
        #     '普卡': 'PK', '金卡': 'JK', '白金卡': 'BJK', '钻石卡': 'ZSK', '钛金卡': 'TJK', '无限卡': 'WXK',
        #     '小白金': 'XBK', '银卡': 'YK', '世界卡': 'SJK', '铂金卡': 'BOJK', '贵宾卡': 'GBK', 'Signature卡': 'SK'
        # }
        # self.type_dict = {
        #     '标准卡': 'BZK', '购物卡': 'GWK', '车主卡': 'CZK', '卡通卡': 'KTK', '商旅卡': 'SLK', '游戏卡': 'YXK',
        #     '多倍积分卡': 'DBJFK', '主题卡': 'ZTK', '影视卡': 'YSK'
        # }
        # self.brand_dict = {
        #     '银联+MasterCard': 'YLK', '银联+JCB': 'YLJCB', 'VISA': 'VISA', 'MasterCard': 'MC', 'JCB': 'JCB', '银联': 'YL',
        #     '运通': 'YT', '银联+运通': 'YL_YT', '银联+VISA': 'YL_VISA', '银联+大来': 'YL_DL'
        # }

    def __shuffle(self, data):
        serial_number = req_for_serial_number(code="CRM_JJK")
        data["ID_"] = serial_number

        # 创建时间及操作人
        time_array = time.localtime()
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
        data["CREATE_TIME_"] = create_time
        data["CREATE_BY_ID_"] = CREATE_ID
        data["CREATE_BY_NAME_"] = CREATE_NAME
        data["M_STATUS_"] = "N"
        data["DELETE_STATUS_"] = "N"
        data["DATA_STATUS_"] = "UNCHECK"
        data["PUBLISH_STATUS_"] = "N"
        data["HOT_"] = "0"
        data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        if source:
            data["SOURCE_"] = source[0]
        data["SOURCE_NAME_"] = data["ENTITY_NAME_"]

        # 处理图片
        if "IMG" in data and data["IMG"]:
            try:
                response = req_for_something(url=data["IMG"])
            except Exception as e:
                self.logger.exception(f"2.1--err: IMG"
                                      f" 原始数据 collection = {self.m_client.mongo_collection};"
                                      f" ENTITY_CODE_ = {data.get('ENTITY_CODE_', 'None')};"
                                      f"error: {e}.")
            else:
                if response:
                    content = response.content
                    encode_data = base64.b64encode(content)
                    data["IMG_"] = encode_data.decode("utf-8")
                response.close()
        else:
            data["IMG_"] = ""

        del data["IMG"]
        del data["DATETIME_"]
        return data

    def generic_shuffle(self, data, field="PRO_NAME_"):
        """
        通用清洗规则写这里, 如不需要通用清洗规则则不继承重写
        :param data:
        :param field:
        :return:
        """
        if isinstance(data, dict):
            re_data = self.__shuffle(data)
            return [{"TABLE_NAME_": self.script_name, "DATA_": re_data}]
        elif isinstance(data, list):
            re_list = list()
            for each in data:
                re_data = self.__shuffle(each)
                re_list.append({"TABLE_NAME_": self.script_name, "DATA_": re_data})
            return re_list
        else:
            return


if __name__ == '__main__':
    # param = sys.argv[1]
    param = "{'entityType':'CRMJPFX_JJK','limitNumber':1,'entityCode':['CRMJPFX_JJK_JTW']}"
    script = BranchOrganize(table_name=TABLE_NAME("CRMJJK"), collection_name="CRMJPFX_JJK", param=param)
    script.main()
    script.close_client()
