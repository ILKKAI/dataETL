# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import jaydebeapi


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-27])

from __config import CREATE_ID, CREATE_NAME, TABLE_NAME
from branch_scripts2 import GenericScript
from tools.req_for_api import req_for_serial_number


TOTAL = 0


class BranchFund(GenericScript):

    def __init__(self, table_name, collection_name, param, verify_field):

        super(BranchFund, self).__init__(table_name, collection_name, param, verify_field=verify_field)
        # 申购状态
        self.bs_dict = {"开放": "KF", "暂停": "ZT", "限大额": "XDE"}
        # new申购状态
        self.new_bs_dict = {"开放申购": "KF", "暂停申购": "ZT", "场内买入": "CNJY", "封闭期": "FBQ",
                            "限制大额申购": "XDE", "暂停交易": "ZTJY"}
        # 基金类型
        self.ft_dict = {"股票型": "GPX", "混合型": "HHX", "债券型": "ZQX", "指数型": "ZSX", "ETF链接": "ETFLJ",
                        "QDII": "QDII", "LOF": "LOF", "FOF": "FOF", "货币型": "HBX", "理财型": "LCX",
                        "分级杠杆": "FJGG", "ETF - 场内": "ETF_CN", "其他": "QT", }

        # 赎回状态
        self.rs_dict = {"场内卖出": "CNJY", "封闭期": "FBQ", "开放赎回": "KFSH", "暂停赎回": "ZTSH", "暂停交易": "ZTJY"}
        # 风险等级
        self.rl_dict = {"低（R1）": "R1", "中低（R2）": "R2", "中（R3）": "R3", "中高（R4）": "R4", "高（R5）": "R5",
                        "未知": "UNKNOWN"}
        self.rl_name_dict = {"低风险": "低（R1）", "中低风险": "中低（R2）", "中风险": "中（R3）",
                             "中高风险": "中高（R4）", "高风险": "高（R5）", "未知": "未知"}

    def fomat_time(self, time_stamp):
        """

        :param time_stamp: 时间戳
        :return: 格式化的时间
        """
        if len(str(time_stamp)) == 13:
            time_stamp = time_stamp // 1000

        return time.strftime('%Y-%m-%d', time.localtime(time_stamp))

    def generic_shuffle(self, data, field="CONTENT_"):
        """
        清洗规则写这里, 如不需要通用清洗规则则不继承
        :param data:
        :param field:
        :return:
        """
        # different shuffle rule
        re_data = dict()

        if "TAGS_" in data:
            re_data["TAGS_"] = ""

        # re_data["HOT_"] = data[""]

        re_data["PRO_NAME_"] = data["PRO_NAME_"]
        re_data["PRO_CODE_"] = data["PRO_CODE_"]
        # 基本信息 插入基本信息表
        if data["ENTITY_CODE_"] in ["JRCP_JJ_TTJJ_FJZ_ALL", "JRCP_JJ_TTJJ_JZ_ALL"]:
            data_dict = dict(TABLE_NAME_=TABLE_NAME("CHA_BRANCH_FUND_BASIC"))
            # self.p_client.table_name = TABLE_NAME("CHA_BRANCH_FUND_BASIC")
            source = re.findall(r"(https?://.*?)[/?]", data["URL_"])
            re_data["SOURCE_"] = source[0]
            re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"]
            # todo
            # re_data["SOURCE_CODE_"] = ""
            re_data["SOURCE_TYPE_"] = data["ENTITY_CODE_"][8:12]
            basic_field_list = ["COM_NAME_", "FUND_TYPE_", "RISK_LEVEL_", "RELEASE_DATE_", "BUILD_DATE_",
                                "BUILD_SCAL_", "ASSET_SCAL_", "SHARE_SCAL_", "MANAGER_", "TRUSTEE_", "HANDLER_",
                                "DIVIDEND_", "MANAGE_FEE_RATE_", "HOST_FEE_RATE_", "SALE_FEE_RATE_", "MAX_SUB_RATE_",
                                "MAX_APPLY_RATE_", "MAX_REDEEM_RATE_", "BENCHMARK_", "BID_", "CLOSE_", "DIM_"]
            for basic_field in basic_field_list:
                if basic_field == "FUND_TYPE_":
                    fund_type = data.get("FUND_TYPE_", "其他")
                    re_data["FUND_TYPE_"] = fund_type
                    try:
                        re_data["FUND_TYPE_CODE_"] = self.ft_dict[data["FUND_TYPE_"]]
                    except KeyError:
                        for ft in self.ft_dict.keys():
                            if ft[:2] in fund_type:
                                re_data["FUND_TYPE_CODE_"] = self.ft_dict[ft]
                        if "FUND_TYPE_CODE_" not in re_data:
                            # self.logger.info(f"FUND_TYPE_CODE_ {fund_type}")
                            re_data["FUND_TYPE_CODE_"] = "QT"
                elif basic_field == "RISK_LEVEL_":
                    risk_level_ = data.get("RISK_LEVEL_", "未知")
                    risk_level_ = re.split(r'[|]', risk_level_.strip())[-1] if risk_level_ else "未知"
                    re_data["RISK_LEVEL_"] = self.rl_name_dict[risk_level_]
                    re_data["RISK_LEVEL_CODE_"] = self.rl_dict.get(re_data["RISK_LEVEL_"], "")
                elif basic_field == "MAX_REDEEM_RATE_":
                    max_redeem_rate_ = data.get("MAX_REDEEM_RATE_", "")
                    re_data["MAX_REDEEM_RATE_"] = re.split(r'[|]', data.get("MAX_REDEEM_RATE_", ""))[-1].replace \
                        ("%", "") if max_redeem_rate_ else ""
                elif basic_field == "BENCHMARK_":
                    re_data[basic_field] = data.get(basic_field, "")
                elif basic_field == "BUILD_DATE_" or basic_field == "RELEASE_DATE_":
                    basic_date = re.findall(r"(\d{4}年\d{2}月\d{1,2})日", data[basic_field])
                    if basic_date:
                        re_data[basic_field] = re.sub(r"[\u4e00-\u9fa5]", "-", basic_date[0])
                else:
                    re_data[basic_field] = data.get(basic_field, "").replace("%", "")
            # 添加一个资产总额字段方便统计
            if re_data["ASSET_SCAL_"]:
                asset_total = re.findall(r"(.*?亿元)（截止至：\d+年\d+月\d+日）", re_data["ASSET_SCAL_"])
                if len(asset_total) > 0:
                    re_data["ASSET_TOTAL_"] = asset_total[0]
                else:
                    re_data["ASSET_TOTAL_"] = '0'
            # 基金基本信息默认都是CHECK
            re_data["DATA_STATUS_"] = "CHECK"
            re_data["DATA_VERSION_"] = "0"

            re_data = super(BranchFund, self).generic_shuffle(data=data, re_data=re_data, field="TRUSTEE_")
            data_dict["DATA_"] = re_data
            return [data_dict]
        # 代销基金 插入代销基金表
        elif "GW_ALL" in data["ENTITY_CODE_"]:
            agency_dict = dict(TABLE_NAME_=TABLE_NAME("CHA_BRANCH_FUND_AGENCY"))
            # self.p_client.table_name = "CHA_BRANCH_FUND_AGENCY"
            # self.p_client.table_name = TABLE_NAME("CHA_BRANCH_FUND_AGENCY")

            serial_number = req_for_serial_number(code="JRCP_JJ_AGENT")
            re_data["ID_"] = serial_number
            source = re.findall(r"(https?://.*?)[/?]", data["URL_"])
            re_data["SOURCE_"] = source[0]
            re_data["SOURCE_NAME_"] = data["ENTITY_NAME_"]
            re_data["PUBLISH_TIME_"] = data["DATETIME_"]
            re_data["SOURCE_TYPE_"] = ""
            # HOT_ 代销基金目前不需要热度字段
            # re_data["HOT_"] = data[""]

            re_data["RECOMMEND_"] = "N"
            re_data["GOOD_SALE_"] = "N"
            re_data["NEW_SALE_"] = "N"
            re_data["PUBLISH_STATUS_"] = "Y"
            re_data["DATA_STATUS_"] = "CHECK"
            re_data["VERSION_"] = "0"
            re_data["DATA_VERSION_"] = "0"
            # 从基金和基金基本信息中获取
            pro_code_ = data.get("PRO_CODE_")
            pro_name = data.get("PRO_NAME_")
            cur = self.connection.cursor()
            # TODO 查取不到
            # 从基金基本信息表查询相关数据
            if pro_code_:
                try:
                    re_data["PRO_CODE_"] = pro_code_
                    detail_list = ["RISK_LEVEL_", "RISK_LEVEL_CODE_", "FUND_TYPE_", "FUND_TYPE_CODE_","BUILD_DATE_"
                                   ,"COM_NAME_", "RELEASE_DATE_", "CLOSE_"]
                    cur.execute(f"SELECT {','.join(detail_list)} "
                                f"FROM CHA_BRANCH_FUND_BASIC WHERE PRO_CODE_='{str(data['PRO_CODE_'])}' "
                                f"ORDER BY CREATE_TIME_ DESC LIMIT 1")
                    for index, item in enumerate(cur.fetchone()):
                        re_data[detail_list[index]] = item
                except Exception as e:
                    re_data["PUBLISH_STATUS_"] = "N"
                    re_data["DATA_STATUS_"] = "UNCHECK"
            elif pro_name:
                try:
                    pro_name = pro_name if not data.get("PRO_LIKE_NAME_") else data.get("PRO_LIKE_NAME_")
                    detail_list = ["PRO_CODE_", "RISK_LEVEL_", "RISK_LEVEL_CODE_", "FUND_TYPE_", "FUND_TYPE_CODE_",
                                   "COM_NAME_", "RELEASE_DATE_", "CLOSE_"]
                    cur.execute(f"SELECT {','.join(detail_list)} "
                                f"FROM CHA_BRANCH_FUND_BASIC WHERE PRO_NAME_ LIKE '{pro_name}%' "
                                f"ORDER BY CREATE_TIME_ DESC LIMIT 1")
                    for index, item in enumerate(cur.fetchone()):
                        re_data[detail_list[index]] = item
                except Exception as e:
                    re_data["PUBLISH_STATUS_"] = "N"
                    re_data["DATA_STATUS_"] = "UNCHECK"
            # 从基金历史净值表查询相关数据
            if re_data.get("PRO_CODE_"):
                try:
                    cur.execute(f"SELECT BUY_STATUS_, NEW_NAV_, NEW_SYR_ "
                                f"FROM CHA_BRANCH_FUND_DATA "
                                f"WHERE PRO_CODE_= '{str(re_data['PRO_CODE_'])}' "
                                f"ORDER BY TIME_ DESC LIMIT 1")
                    re_data["BUY_STATUS_"], re_data["NEW_NAV_"], re_data["NEW_SYR_"] = cur.fetchone()
                    if re_data["BUY_STATUS_"] and re_data["BUY_STATUS_"] in self.new_bs_dict.keys():
                        re_data["BUY_STATUS_CODE_"] = self.new_bs_dict[re_data["BUY_STATUS_"]]
                except Exception as e:
                    re_data["PUBLISH_STATUS_"] = "N"
                    re_data["DATA_STATUS_"] = "UNCHECK"

                finally:
                    cur.close()
            if not re_data.get("RISK_LEVEL_"):
                if "RISK_LEVEL_" not in data:
                    risk_level_ = "未知"
                else:
                    risk_level_ = data["RISK_LEVEL_"]
                risk_level_ = re.split(r'[|]', risk_level_.strip())[-1] if risk_level_ else "未知"
                re_data["RISK_LEVEL_"] = self.rl_name_dict[risk_level_]
                re_data["RISK_LEVEL_CODE_"] = self.rl_dict.get(re_data["RISK_LEVEL_"], "")
            # re_data["NEW_SYR_"] = data[""]
            if not (re_data.get("FUND_TYPE_") or re_data.get("RELEASE_DATE_")):
                re_data["PUBLISH_STATUS_"] = "N"
                re_data["DATA_STATUS_"] = "UNCHECK"
            re_data = super(BranchFund, self).generic_shuffle(data=data, re_data=re_data, field="ENTITY_NAME_")
            agency_dict["DATA_"] = re_data
            return [agency_dict]
        # 历史净值 插入基金表
        elif data["ENTITY_CODE_"] in ["JRCP_JJ_TTJJ_FJZ", "JRCP_JJ_TTJJ_JZ"]:
            serial_number = req_for_serial_number(code=data["ENTITY_CODE_"][:7])
            re_data["ID_"] = serial_number
            # re_data["FUND_BASIC_ID_"] = data[""]
            nom_field_list = ["TIME_", "NEW_NAV_", "NEW_ANV_", "OLD_TIME_", "OLD_NAV_", "OLD_ANV_", "DAY_GROWTH_",
                              "DAY_GROWTH_RATE_", "ONE_MONTH_RATE_", "THREE_MONTH_RATE_", "SIX_MONTH_RATE_",
                              "ONE_YEAR_RATE_", "THREE_YEAR_RATE_", "BUILD_RATE_", "NEW_TOI_", "NEW_SYR_", "OLD_TOI_",
                              "OLD_SYR_", "FYR_", "TYR_", "MARKET_PRICE_", "DISCOUNT_RATE_", "VERSION_",
                              "BUY_STATUS_", "REDEEM_STATUS_"]
            for nom_field in nom_field_list:
                if nom_field == "VERSION_":
                    re_data[nom_field] = "0"
                elif nom_field == "BUY_STATUS_":
                    re_data["BUY_STATUS_"] = data.get("BUY_STATUS_", "")
                    re_data["BUY_STATUS_CODE_"] = self.new_bs_dict.get(re_data["BUY_STATUS_"], "")
                elif nom_field == "REDEEM_STATUS_":
                    re_data["REDEEM_STATUS_"] = data.get("REDEEM_STATUS_")
                    re_data["REDEEM_STATUS_CODE_"] = self.rs_dict.get(re_data["REDEEM_STATUS_"], "")
                else:
                    re_data[nom_field] = data.get(nom_field, "").replace("%", "")
                    re_data[nom_field] = re_data[nom_field].replace("--", "")
            re_data["CREATE_BY_ID_"] = CREATE_ID
            re_data["CREATE_BY_NAME_"] = CREATE_NAME
            # 处理T-1日净值
            # self.p_client.table_name =
            cur = self.connection.cursor()
            cur.execute(f"SELECT NEW_NAV_,NEW_ANV_,NEW_TOI_,NEW_SYR_ FROM  {TABLE_NAME('CHA_BRANCH_FUND_DATA')} where PRO_CODE_='{re_data['PRO_CODE_']}' and TIME_<'{re_data['TIME_']}' order by TIME_ desc limit 1")
            t_1data = cur.fetchone()
            if t_1data:
                self.logger.info(f"====T-1日数据===={t_1data}")
                # print(t_1data)
                re_data['OLD_NAV_'] = t_1data[0]
                re_data['OLD_ANV_'] = t_1data[1]
                re_data['OLD_TOI_'] = t_1data[2]
                re_data['OLD_SYR_'] = t_1data[3]

            # 更新代销基金数据
            self.p_client.table_name = TABLE_NAME('CHA_BRANCH_FUND_AGENCY')
            agences = self.p_client.search_all_from_phoenix(connection=self.connection, dict_status=True,
                                                            where_condition=f"PRO_CODE_='{re_data['PRO_CODE_']}'")
            if agences:
                while True:
                    try:
                        agence_data = agences.__next__()
                        self.logger.info(f"====更新代销基金数据===={agence_data}")
                        agence_data['NEW_NAV_'] = re_data['NEW_NAV_']
                        agence_data['NEW_SYR_'] = re_data['NEW_SYR_']
                        agence_data['BUY_STATUS_'] = re_data['BUY_STATUS_']
                        agence_data['BUY_STATUS_CODE_'] = re_data['BUY_STATUS_CODE_']
                    except StopIteration:
                        break
                    try:
                        self.p_client.upsert_to_phoenix_by_one(connection=self.connection, data=agence_data)
                    except jaydebeapi.DatabaseError:
                        continue
            self.p_client.table_name = TABLE_NAME('CHA_BRANCH_FUND_DATA')
            return [{"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_FUND_DATA"), "DATA_": re_data}]


if __name__ == '__main__':
    # param = sys.argv[1]
    # '''
    # 基金需要查询相关 PRO_CODE_
#     '''害死猫:
# 小帅,,天天基金的数据是不是要先跑   ALL 的在跑  历史净值的啊
#
# 害死猫:
# 就是先跑    天天基金 BASIC 表   在跑历史净值
#
# drift:
# 都是分开的
#
# drift:
# basic的是之前一次性跑的，你根据实体编码问飞哥，飞哥知道
# '''
    # '''
    param = "{'entityType':'JRCP_JJ','limitNumber':1000,'entityCode':['JRCP_JJ_HEBYH_GW_ALL',]}"

    # print(param)
    # 天天基金 BASIC 表
    if "JRCP_JJ_TTJJ_FJZ_ALL" in param or "JRCP_JJ_TTJJ_JZ_ALL" in param:
        table_name = "CHA_BRANCH_FUND_BASIC"
        collection = "JRCP_JJ"
        verify_field = {"URL_": "URL_"}
    # 代销
    elif "GW_ALL" in param and "TTJJ" not in param:
        table_name = "CHA_BRANCH_FUND_AGENCY"
        collection = "JRCP_JJ"
        verify_field = {"URL_": "URL_"}  # 用于mongo 去重查询
    # 历史净值
    else:
        table_name = "CHA_BRANCH_FUND_DATA"
        collection = "JRCP_JJ"
        # 验证字段，key是hbase的字段。value是洗出来数据的key。拼接后为：PRO_CODE_='000406' and TIME_='2019-05-30'
        verify_field = {'PRO_CODE_': 'PRO_CODE_', 'TIME_': 'TIME_'}
    script = BranchFund(table_name=TABLE_NAME(table_name), collection_name=collection, param=param, verify_field=verify_field)
    script.main()
    script.close_client()
