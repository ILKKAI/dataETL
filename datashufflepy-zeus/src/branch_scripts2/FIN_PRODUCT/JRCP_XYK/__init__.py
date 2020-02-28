# -*- coding: utf-8 -*-
import base64
import os
import re
import sys


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-27])

from branch_scripts2 import GenericScript
from tools.req_for_api import req_for_serial_number, req_for_something
from __config import TABLE_NAME


class BranchXyk(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(BranchXyk, self).__init__(table_name, collection_name, param, verify_field=verify_field)
        # BANK_NAME_ 字典
        self.name_dict = {'工行': 'ICBC', '工商银行': 'ICBC', '农行': 'ABC', '农业银行': 'ABC', '中行': 'BOC', '中银': 'BOC', '中国银行': "BOC",
                        '建行': 'CCB', '建信': 'CCB', '建设银行': 'CCB', '交行': 'BCM', '交通银行': 'BCM', '邮储银行': 'PSBC', '浙商银行': 'CZB',
                        '渤海银行': 'CBHB', '中信银行': 'ECITIC', '光大银行': 'CEB', '华夏银行': 'HB', '招行': 'CMB', '招商银行': 'CMB',
                        '兴业银行': 'CIB', '广发银行': 'CGB', '平安银行': 'PAB', '浦发银行': 'SPDB', '恒丰银行': 'EBCL',
                        '浦东发展银行': 'SPDB', '民生银行': 'CMBC', '汇丰银行': 'HSBC', '渣打银行': 'SC'}
        self.level_dict = {
            '普卡': 'PK', '金卡': 'JK', '白金卡': 'BJK', '钻石卡': 'ZSK', '钛金卡': 'TJK', '无限卡': 'WXK',
            '小白金': 'XBK', '银卡': 'YK', '世界卡': 'SJK', '铂金卡': 'BOJK', '贵宾卡': 'GBK', 'Signature卡': 'SK'
        }
        self.type_dict = {
            '标准卡': 'BZK', '购物卡': 'GWK', '车主卡': 'CZK', '卡通卡': 'KTK', '商旅卡': 'SLK', '游戏卡': 'YXK',
            '多倍积分卡': 'DBJFK', '主题卡': 'ZTK', '影视卡': 'YSK'
        }
        self.brand_dict = {
            '银联+MasterCard': 'YLK', '银联+JCB': 'YLJCB', 'VISA': 'VISA', 'MasterCard': 'MC', 'JCB': 'JCB', '银联': 'YL',
            '运通': 'YT', '银联+运通': 'YL_YT', '银联+VISA': 'YL_VISA', '银联+大来': 'YL_DL'
        }
        self.use_person_dict = {
            '学生': 'XS', '白领': 'BL', '高端系列': 'GDXL', '商务人士': 'SWRS'
        }

    def generic_shuffle(self, data):
        """
        清洗规则写这里，如不需要通用清洗规则则不继承, 从大文本中筛选数据
        :param data:
        :param field:
        :return:
        """
        re_data = dict()
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        re_data["URL_"] = data["URL_"]

        serial_number = req_for_serial_number(code="JRCP_XYK")
        re_data["ID_"] = serial_number
        # 时间维度
        re_data["PERIOD_CODE_"] = data["DATETIME_"][:10].replace("-", "")

        source = re.findall(r"(https?://.*?)/", data["URL_"])
        re_data["SOURCE_"] = source[0]
        re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"]
        re_data["SOURCE_TYPE_"] = "WAK"

        # 对特殊微信 BANK_NAME 做处理
        for key, value in self.name_dict.items():
            if key[:2] in data["PRO_NAME_"]:
                re_data["BANK_NAME_"] = key
                re_data["BANK_CODE_"] = value
                break
        if "BANK_NAME_" in re_data:
            if re_data["BANK_NAME_"] == "建信":
                re_data["BANK_NAME_"] = "中国建设银行"
            if re_data["BANK_NAME_"] == "建行":
                re_data["BANK_NAME_"] = "中国建设银行"
            if re_data["BANK_NAME_"] == "建设银行":
                re_data["BANK_NAME_"] = "中国建设银行"
            if re_data["BANK_NAME_"] == "农行":
                re_data["BANK_NAME_"] = "中国农业银行"
            if re_data["BANK_NAME_"] == "农业银行":
                re_data["BANK_NAME_"] = "中国农业银行"
            if re_data["BANK_NAME_"] == "工行":
                re_data["BANK_NAME_"] = "中国工商银行"
            if re_data["BANK_NAME_"] == "工商银行":
                re_data["BANK_NAME_"] = "中国工商银行"
            if re_data["BANK_NAME_"] == "民生银行":
                re_data["BANK_NAME_"] = "中国民生银行"
            if re_data["BANK_NAME_"] == "光大银行":
                re_data["BANK_NAME_"] = "中国光大银行"
            if re_data["BANK_NAME_"] == "交行":
                re_data["BANK_NAME_"] = "交通银行"
            if re_data["BANK_NAME_"] == "招行":
                re_data["BANK_NAME_"] = "招商银行"
            if re_data["BANK_NAME_"] == "农行":
                re_data["BANK_NAME_"] = "中国农业银行"
            if re_data["BANK_NAME_"] == "中行":
                re_data["BANK_NAME_"] = "中国银行"
            if re_data["BANK_NAME_"] == "中银":
                re_data["BANK_NAME_"] = "中国银行"
            if re_data["BANK_NAME_"] == "邮储银行":
                re_data["BANK_NAME_"] = "中国邮政储蓄银行"

        # 信用卡名称
        if "PRO_NAME_" in data:
            if "(" in data["PRO_NAME_"]:
                data["PRO_NAME_"] = data["PRO_NAME_"][:data["PRO_NAME_"].find("(")]
            elif "（" in data["PRO_NAME_"]:
                data["PRO_NAME_"] = data["PRO_NAME_"][:data["PRO_NAME_"].find("（")]
            re_data["PRO_NAME_"] = data["PRO_NAME_"]
        # 卡币种
        if "CURRENCY_TYPE_" in data:
            re_data["CURRENCY_TYPE_"] = data["CURRENCY_TYPE_"]
        # 卡币种类型
        if data["CURRENCY_TYPE_"] == "人民币":
            re_data["CURRENCY_TYPE_CODE_"] = "RMB"
        if re.match(r"人民币/.*?", data["CURRENCY_TYPE_"]):
            re_data["CURRENCY_TYPE_CODE_"] = "DBZ"
        if data["CURRENCY_TYPE_"] == "美元":
            re_data["CURRENCY_TYPE_CODE_"] = "DBZ"
        # 卡组织|结算渠道
        if "BRAND_" in data:
            re_data["BRAND_"] = data["BRAND_"]

        # 卡组织CODE
        for brand_key in self.brand_dict:
            if brand_key in data["BRAND_"]:
                re_data["BRAND_CODE_"] = self.brand_dict[brand_key]
                break

        # 卡等级
        if "LEVEL_" in data:
            re_data["LEVEL_"] = data["LEVEL_"]
        # 卡等级CODE
        for level_key in self.level_dict:
            if level_key[:2] in data["LEVEL_"][:2]:
                re_data["LEVEL_CODE_"] = self.level_dict[level_key]
                break
        # 取现额度
        if "CONSUME_LIMIT_" in data:
            re_data["CONSUME_LIMIT_"] = data["CONSUME_LIMIT_"]

        # 这里开始从大文本清洗
        # 免息期
        GRACE_PERIODS_ = re.findall(r".*?免息期[:：]\|(.*?)\|", data["CONTENT_"])
        if len(GRACE_PERIODS_) > 0:
            GRACE_PERIODS_ = GRACE_PERIODS_[0]
            # 处理到20天50天的错误数据
            pattern = re.compile(r"到(\d+)天(\d+)天")
            if re.match(pattern, GRACE_PERIODS_):
                GRACE_PERIODS_ = pattern.sub(r"\1天到\2天", GRACE_PERIODS_)

            if GRACE_PERIODS_ == "消费验证方式：":
                GRACE_PERIODS_ = ""

            if GRACE_PERIODS_ == "预借现金额度：" or GRACE_PERIODS_ == "预借现金额度:":
                GRACE_PERIODS_ = ""
            if re.match(r"最长\d+天最长\d+天", GRACE_PERIODS_):
                a = re.match(r"(最长\d+天)最长\d+天", GRACE_PERIODS_)
                GRACE_PERIODS_ = a.group(1)

            if re.match(r"\d+天到\d+天\d+天到\d+天", GRACE_PERIODS_):
                a = re.match(r"(\d+天)到(\d+天)(\d+天)到\d+天", GRACE_PERIODS_)
                if a.group(1) == a.group(2):
                    GRACE_PERIODS_ = a.group(1) + "到" + a.group(3)
                else:
                    GRACE_PERIODS_ = a.group(1) + "到" + a.group(2)

            if re.match(r"\d+天\d+天\d+天\d+天", GRACE_PERIODS_):
                a = re.match(r"(\d+天)\d+天(\d+天)\d+天", GRACE_PERIODS_)
                GRACE_PERIODS_ = a.group(1) + "到" + a.group(2)

            if re.match(r"\d+天\d+天", GRACE_PERIODS_):
                a = re.match(r"(\d+天)(\d+天)", GRACE_PERIODS_)
                GRACE_PERIODS_ = a.group(1) + "到" + a.group(2)

            if re.match(r"至\d+天\d+天", GRACE_PERIODS_):
                a = re.match(r"至(\d+天)(\d+天)", GRACE_PERIODS_)
                GRACE_PERIODS_ = a.group(1) + "到" + a.group(2)

            re_data["GRACE_PERIODS_"] = GRACE_PERIODS_
        else:
            re_data["GRACE_PERIODS_"] = data["GRACE_PERIODS_"]

        # 免年费政策
        FREE_POLICY_ = re.findall(r".*?免年费政策[:：]\|(.*?)\|", data["CONTENT_"])
        if len(FREE_POLICY_) > 0:
            FREE_POLICY_ = FREE_POLICY_[0]
            # 删除重复数据
            pattern = re.compile(r"(免\d+年年费){2,9}")
            if re.match(pattern, FREE_POLICY_):
                a = re.match(pattern, FREE_POLICY_)
                FREE_POLICY_ = a.group(1)
            pattern = re.compile(r"(终身免年费){2,9}")
            if re.match(pattern, FREE_POLICY_):
                a = re.match(pattern, FREE_POLICY_)
                FREE_POLICY_ = a.group(1)
            re_data["FREE_POLICY_"] = FREE_POLICY_

        # 主卡年费
        FEE_ = re.findall(r".*?主卡年费[:：]\|(.*?)\|", data["CONTENT_"])
        if len(FEE_) > 0:
            FEE_ = FEE_[0]
            tempfee = re.findall(r".*?(\d+).*?", FEE_)
            if len(tempfee) > 0:
                re_data["FEE_"] = tempfee[0]
            else:
                re_data["FEE_"] = ""
        else:
            re_data["FEE_"] = "0"

        # 预借现金额度
        PRE_BORROW_ = re.findall(r".*?预借现金额度[:：]\|(.*?)\|", data["CONTENT_"])
        if len(PRE_BORROW_) > 0:
            PRE_BORROW_ = PRE_BORROW_[0]
            if PRE_BORROW_ == "免息期：":
                PRE_BORROW_ = ""
            if PRE_BORROW_ == "免年费政策：":
                PRE_BORROW_ = ""
            # 去除重复的数据
            pattern = re.compile(r"(信用额度的\d+%)信用额度的\d+%")
            if re.match(pattern, PRE_BORROW_):
                a = re.match(pattern, PRE_BORROW_)
                PRE_BORROW_ = a.group(1)

            pattern = re.compile(r"(信用额度的\d+-\d+%)信用额度的\d+%")
            if re.match(pattern, PRE_BORROW_):
                a = re.match(pattern, PRE_BORROW_)
                PRE_BORROW_ = a.group(1)

            pattern = re.compile(r"(普卡信用额度的\d+%)白金卡信用额度的\d+%金卡信用额度的\d+%")
            if re.match(pattern, PRE_BORROW_):
                a = re.match(pattern, PRE_BORROW_)
                PRE_BORROW_ = a.group(1)

            pattern = re.compile(r"(普卡信用额度的\d+%)金卡信用额度的\d+%")
            if re.match(pattern, PRE_BORROW_):
                a = re.match(pattern, PRE_BORROW_)
                PRE_BORROW_ = a.group(1)

            pattern = re.compile(r"(白金卡信用额度的\d+%)金卡信用额度的\d+%")
            if re.match(pattern, PRE_BORROW_):
                a = re.match(pattern, PRE_BORROW_)
                PRE_BORROW_ = a.group(1)

            re_data["PRE_BORROW_"] = PRE_BORROW_
        else:
            re_data["PRE_BORROW_"] = ""

        # 消费验证方式
        re_data["VALID_CONSUME_"] = "密码+签名 签名"

        # 账单日
        BILL_DATE_ = re.findall(r".*?账单日[:：]\|(.*?)\|", data["CONTENT_"])
        if len(BILL_DATE_) > 0:
            BILL_DATE_ = BILL_DATE_[0]
            # 处理重复的账单日数据 比如:账单日21号账单日21号账单日21号
            pattern = re.compile(r"(账单日\d+号){2,9}")
            if re.match(pattern, BILL_DATE_):
                a = re.match(pattern, BILL_DATE_)
                BILL_DATE_ = a.group(1)
            re_data["BILL_DATE_"] = BILL_DATE_
        else:
            re_data["BILL_DATE_"] = ""

        # 积分方式
        POINTS_ = re.findall(r".*?积分方式[:：]\|(.*?)\|", data["CONTENT_"])
        if len(POINTS_) > 0:
            POINTS_ = POINTS_[0]
            if re_data.get("BANK_CODE_") and re_data["BANK_CODE_"] == "CMB":
                POINTS_ = POINTS_.replace("元", "元 ")
            else:
                POINTS_ = POINTS_.replace("分", "分 ")
                POINTS_ = POINTS_.replace("倍", "倍 ")
                POINTS_ = POINTS_.replace("积分 的2倍", "积分的2倍")
            re_data["POINTS_"] = POINTS_
        else:
            re_data["POINTS_"] = ""

        # 积分有效期
        VALID_DATE_POINTS_ = re.findall(r".*?积分有效期[:：]\|(.*?)\|", data["CONTENT_"])
        if len(VALID_DATE_POINTS_) > 0:
            VALID_DATE_POINTS_ = VALID_DATE_POINTS_[0]
            # 给几组有效期之间加上空格
            pattern = re.compile(r"(白金卡\d+年)(金卡\d+年)(普卡\d+年)")
            if re.match(pattern, VALID_DATE_POINTS_):
                VALID_DATE_POINTS_ = re.sub(pattern, r"\1 \2 \3", VALID_DATE_POINTS_)

            pattern = re.compile(r"(\d+年到\d+年)(\d+年)(永久有效)")
            if re.match(pattern, VALID_DATE_POINTS_):
                VALID_DATE_POINTS_ = re.sub(pattern, r"\1 \2 \3", VALID_DATE_POINTS_)

            re_data["VALID_DATE_POINTS_"] = VALID_DATE_POINTS_
        else:
            re_data["VALID_DATE_POINTS_"] = ""

        # 循环信用利息
        DAILY_INTEREST_ = re.findall(r".*?循环信用利息（日息）[:：]?\|(.*?)\|", data["CONTENT_"])
        if len(DAILY_INTEREST_) > 0:
            DAILY_INTEREST_ = DAILY_INTEREST_[0]
            if DAILY_INTEREST_ == "消费短信通知费：":
                DAILY_INTEREST_ = ""
            re_data["DAILY_INTEREST_"] = DAILY_INTEREST_
        else:
            re_data["DAILY_INTEREST_"] = ""

        # 最低还款
        MIN_REPAY_ = re.findall(r".*?最低还款[:：]?\|(.*?)\|", data["CONTENT_"])
        if len(MIN_REPAY_) > 0:
            MIN_REPAY_ = MIN_REPAY_[0]
            if re.match(r"最低应还所欠金额的\d+%最低应还所欠金额的\d+%", MIN_REPAY_):
                a = re.match(r"(最低应还所欠金额的\d+%)最低应还所欠金额的\d+%", MIN_REPAY_)
                MIN_REPAY_ = a.group(1)
            if MIN_REPAY_ == "账单日：":
                MIN_REPAY_ = ""
            re_data["MIN_REPAY_"] = MIN_REPAY_
        else:
            re_data["MIN_REPAY_"] = ""

        # 卡片特色
        if "SPECIAL_" in data and len(data["SPECIAL_"]) > 0:
            re_data["SPECIAL_"] = data["SPECIAL_"].replace("|", "<br/>")

        # 增值服务
        if "VAS_" in data and len(data["VAS_"]) > 0:
            re_data["VAS_"] = data["VAS_"].replace("|", "<br/>")

        # 信用卡图片
        # 处理错误的信用卡图片URL
        if "IMAGES_" in data:
            pattern = re.compile(r"https:(http://.*)")
            if re.match(pattern, data["IMAGES_"]):
                a = re.match(pattern, data["IMAGES_"])
                image_url = a.group(1)
            else:
                image_url = data["IMAGES_"]
            response = req_for_something(url=image_url)
            if response:
                t = base64.b64encode(response.content)
                re_data["IMAGE_"] = t.decode("utf-8")

        re_data = super(BranchXyk, self).generic_shuffle(data=data, re_data=re_data, field=None)
        # print(re_data)
        re_data["PUBLISH_TIME_"] = re_data["SPIDER_TIME_"]
        return [{"TABLE_NAME_": self.script_name, "DATA_": re_data}]


if __name__ == '__main__':
    param = sys.argv[1]
    # param = "{'limitNumber':'1000'}"
    verify_field = {"URL_": "URL_"}
    script = BranchXyk(table_name=TABLE_NAME("CHA_BRANCH_CREDITCARDARD"), collection_name="JRCP_XYK", param=param,
                       verify_field=verify_field)
    script.main()
    script.close_client()
