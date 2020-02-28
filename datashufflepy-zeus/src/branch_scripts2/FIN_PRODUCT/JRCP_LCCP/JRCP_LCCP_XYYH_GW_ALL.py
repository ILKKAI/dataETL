# -*- coding: utf-8 -*-
"""兴业银行 理财产品 JRCP_LCCP_XYYH_GW_ALL"""
import re

from database._mongodb import MongoClient


def data_shuffle(data):
    re_list = list()
    content = re.findall(r"[功网][能银][购操][买作]\)?\|(.*)", data["CONTENT_"])
    each = re.findall(r".*?\|购买\|", content[0])
    for item in each:
        re_data = dict()
        re_data["SALE_SOURCE_"] = data["SALE_SOURCE_"]
        re_data["URL_"] = data["URL_"]
        re_data["DEALTIME_"] = data["DEALTIME_"]
        re_data["DATETIME_"] = data["DATETIME_"]
        re_data["ENTITY_NAME_"] = data["ENTITY_NAME_"]
        re_data["ENTITY_CODE_"] = data["ENTITY_CODE_"]
        re_data["_id"] = data["_id"]
        item_s = item.split("|")
        time_list = list()
        # 产品名称
        re_data["PRO_NAME_"] = item_s[0]
        for i in item_s:
            # 时间
            if re.findall(r"\d+/\d+", i):
                j = i.split("/")
                k = j
                for jj in j:
                    if len(jj) == 1:
                        k[j.index(jj)] = "0"+jj
                if "2019" not in k:
                    k.insert(0, "2019")
                time_list.append("-".join(k))
            elif re.findall(r"2019\d{4}", i):
                j = "-".join([i[:4], i[4:6], i[6:]])
                time_list.append(j)
            elif re.findall(r"\d{4}年\d{1,2}月\d{1,2}日-\d{1,2}月\d{1,2}日", i):
                j = i.split("-")
                for k in j:
                    l = k.replace("日", "")
                    l = l.replace("年", "-")
                    l = l.replace("月", "-")
                    time_list.append(l)
            # 募集币种
            elif re.findall(r"[人美][民元]币?", i):
                re_data["CURRENCY_TYPE_"] = re.findall(r"[人美][民元]币?", i)[0]
            # 起购金额
            elif re.findall(r"^>?\d+.?\d*万$", i):
                i = i.replace(">", "")
                if "." in i:
                    re_data["START_FUNDS_"] = i.replace(".", "")
                    re_data["START_FUNDS_"] = re_data["START_FUNDS_"].replace("万", "0000")
                    re_data["START_FUNDS_"] = str(int(re_data["START_FUNDS_"]))
                else:
                    re_data["START_FUNDS_"] = i
            # 收益率
            elif re.findall(r"^\d+\.?\d*%-?\d*\.?\d*%?$", i):
                yield_l = i.split("-")
                if len(yield_l) == 1:
                    re_data["YIELD_HIGH_"] = yield_l[0]
                else:
                    re_data["YIELD_HIGH_"] = yield_l[0]
                    re_data["YIELD_LOW_"] = yield_l[1]
            # 期限
            elif re.findall(r"^\d{2,}$", i):
                re_data["REAL_DAYS_"] = i
            elif re.findall(r"^>?(\d{1,2})个月$", i):
                real_days = re.findall(r"^>?(\d{1,2})个月$", i)[0]
                re_data["REAL_DAYS_"] = str(int(real_days) * 30)
            elif "净值型" in i or "浮动收益" in i:
                re_data["OPT_MODE_"] = i
            elif "全国" in i:
                re_data["SALE_AREA_"] = i
        # print(data["URL_"])
        if time_list:
            if len(time_list) == 2:
                re_data["RAISE_START_"] = time_list[0]
                re_data["RAISE_END_"] = time_list[1]
            else:
                pass
        re_list.append(re_data)
                 # print(data["URL_"])
    # print(data["URL_"])
    return re_list


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="JRCP_LCCP_XYYH_GW_ALL", mongo_collection="JRCP_LCCP")
    data_list = main_mongo.main()
    for data in data_list:
        re_list = data_shuffle(data)
        for each in re_list:
            print(each)
            # break


"""
现金宝|发售现金宝系列理财产品简介|现金宝交易时间为：|工作日7:30~15:45（其中添利1号、添利3号为9:00~15:45）|。|网银购买理财产品限于最近一年已在本行柜面进行了风险评估的客户。|产品名称|销售地区|币种|期限（天）|产品类型|起购金额（元）|客户年化参考净收益率|在线购买|(请使用手机银行客户端左上角“扫一扫”功能购买)|现金宝·添利1号净值型理财产品|全国|人民币|每日申赎，收益率波动|净值型|1|万|以我行网站刊登的参考收益率公告为准|现金宝·添利3号净值型理财产品（简称：添利日日生金）|全国|人民币|每日申赎，收益率波动|净值型|1|万|以我行网站刊登的参考收益率公告为准|现金宝1号理财产品|全国|人民币|每日申赎，收益率波动|非保本浮动收益|1万|以我行网站刊登的参考收益率公告为准|购买|现金宝3号理财产品|全国|人民币|每日申赎，收益递进|非保本浮动收益|1万|以我行网站刊登的参考收益率公告为准|现金宝4号私人银行类理财产品|全国|人民币|每日申赎，收益递进|非保本浮动收益|30万|以我行网站刊登的参考收益率公告为准|回到顶部|
"""
