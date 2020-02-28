# -*- coding: utf-8 -*-
import os
import re
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath[:-16])
sys.path.append(rootPath[:-16])

from __config import TABLE_NAME
from branch_scripts2 import GenericScript
from tools.req_for_api import req_for_serial_number
from tools.web_api_of_baidu import get_lat_lng, get_area


class MapbarScript(GenericScript):
    def __init__(self, table_name, collection_name, param, verify_field):
        super(MapbarScript, self).__init__(table_name, collection_name, param, verify_field=verify_field)
        self.source_type2_dict = {'中餐馆': 'ZCG', '西餐馆': 'XCG', '快餐店': 'KCD', '酒吧': 'JB', '茶馆': 'CG', '茶庄': 'CZ', '咖啡馆': 'KFG', '冷饮': 'LY', '西点': 'XD', '蛋糕': 'DG', '亚洲菜': 'YZC', '其他餐饮服务': 'QTCYFW', '其他旅游景点': 'QTLYJD', '游乐园': 'YLY', '植物园': 'ZWY', '动物园': 'DWY', '风景名胜': 'FJMS', '展览馆': 'ZLG', '会展中心': 'HZZX', '博物馆': 'BWG', '风景区': 'FJQ', '水系': 'SX', '水族馆': 'SZG', '教堂': 'JT', '公园': 'GY', '综合性广场': 'ZHXGC', '纪念馆': 'JNG', '民俗村': 'MSC', '农家乐': 'NJL', '名人故居': 'MRGJ', '清真寺': 'QZS', '景点的出入口': 'JDDCRK', '寺庙': 'SM', '道观': 'DG', '塔': 'T', '体育馆': 'TYG', '游泳': 'YY', '球类馆': 'QLG', '溜冰场': 'LBC', '滑雪场': 'HXC', '高尔夫球场': 'GEFQC', '其他运动健身': 'QTYDJS', '户外运动俱乐部': 'HWYDJLB', '赛马场及马术俱乐部': 'SMCJMSJLB', '室内运动健身俱乐部': 'SNYDJSJLB', '加油加气站': 'JYJQZ', '充电站': 'CDZ', '汽车销售': 'QCXS', '汽车租赁': 'QCZL', '汽车用品': 'QCYP', '汽车维修': 'QCWX', '养护与美容': 'HYYMR', '洗车场': 'XCC', '机动车检测场': 'JDCJCC', '电子游戏厅及网吧': 'DZYXTJWB', 'KTV': 'KTV', '音乐厅': 'YYT', '娱乐城': 'YLC', '歌舞厅': 'GWT', '夜总会': 'YZH', '保龄球馆': 'BLQG', '洗浴中心': 'XYZX', '海滨浴场': 'HBYC', '电影院': 'DYY', '演出场所售票处': 'YCCSSPC', '剧场': 'JC', '休闲运动': 'XXYD', '棋牌室': 'QPS', '台球室': 'TQS', '美容美发': 'MRMF', '足疗': 'ZL', '按摩': 'AM', '其他休闲娱乐': 'QTXXYL', '非星级度假村': 'FXJDJC', '疗养院': 'LYY', '星级酒店': 'XJJD', '旅馆': 'LG', '招待所': 'ZDS', '经济型连锁酒店': 'JJXLSJD', '其他宾馆饭店': 'QTBGFD', '超市': 'CS', '连锁店': 'LSJD', '品牌服饰专卖': 'PPFSZM', '孕婴幼用品专卖': 'YYYYPZM', '其他服装鞋帽': 'QTFZPM', '烟酒、饮料专卖': 'YJYLZM', '地方特产专卖': 'DDTCZM', '粮油食品': 'LYSP', '农副产品': 'NFCP', '水产市场': 'SCSC', '家居建材': 'JJJC', '图书': 'TS', '音像制品': 'YXZP', '电子商场': 'DZSC', '电器商场': 'DQSC', '商铺': 'SP', '钟表/眼镜': 'ZBYJ', '鲜花': 'XH', '珠宝': 'ZB', '工艺品及古玩字画': 'GYPJGWZH', '洗衣': 'XY', '便利店': 'BLD', '商业街': 'SYJ', '小商品市场': 'XSPSC', '中小学校': 'ZXXX', '大学院校': 'DXYX', '校内附属设施': 'YNFSSS', '科研院所': 'KYYS', '文化馆': 'WHG', '活动中心': 'HDZX', '文化宫': 'WHG', '俱乐部': 'JLB', '幼儿园': 'YEY', '图书馆': 'TSG', '档案馆': 'DAG', '宫': 'QSNG', '科技馆': 'KJG', '天文馆': 'TWG', '美术馆/画廊': 'MSGHL', '艺术团体': 'YSTT', '其他培训中心': 'QTPXZX', '技术类学校': 'JSLXX', '特殊教育': 'TSJY', '职业教育': 'ZYJY', '出国留学': 'CGLX', '艺术培训': 'YSPX', '继续教育': 'JXJY', '其他文化教育': 'QTWHJY', '地铁站': 'DTZ', '交车站': 'GJCZ', '火车站': 'HCZ', '城市轨道交通出入口': 'CSGDJTCRK', '机场': 'JC', '机场附属设施': 'JCFSSS', '码头': 'MT', '渡口': 'DK', '高速公路出入口': 'GSGLCRK', '高速公路服务区': 'GSGLFWQ', '收费站': 'SFZ', '道路': 'DL', '立交桥': 'LJQ', '停车场': 'TCC', '路口': 'LK', '交通运输': 'JTYS', '长途客运站': 'CTKY', '桥梁': 'QL', '隧道': 'SD', '中国电信': 'ZGDX', '邮局': 'YJ', '邮政速递': 'YZSD', '其他邮电通信': 'QTYDTX', '中国移动': 'ZGYD', '中国联通': 'ZGLT', '社区医疗': 'SQYL', '药店': 'YD', '综合医院': 'ZHYY', '专科医院': 'ZKYY', '急救中心': 'JJZX', '康复中心': 'KFZX', '诊所/卫生所': 'ZSWSS', '疾病防治': 'JBFZ', '疾病预防中心': 'JBYFZX', '门诊': 'MZ', '急诊部': 'JZB'}

        self.type2_dict = {'中餐馆': 'ZCG', '西餐馆': 'XCG', '快餐店': 'KCD', '酒吧': 'JB', '茶馆': 'CG', '茶庄': 'CZ', '咖啡馆': 'KFG', '冷饮': 'LY', '西点': 'XD', '蛋糕': 'DG', '亚洲菜': 'YZC', '其他餐饮服务': 'QTCYFW', '其他旅游景点': 'QTLYJD', '游乐园': 'YLY', '植物园': 'ZWY', '动物园': 'DWY', '风景名胜': 'FJMS', '展览馆': 'ZLG', '会展中心': 'HZZX', '博物馆': 'BWG', '风景区': 'FJQ', '水系': 'SX', '水族馆': 'SZG', '教堂': 'JT', '公园': 'GY', '综合性广场': 'ZHXGC', '纪念馆': 'JNG', '民俗村': 'MSC', '农家乐': 'NJL', '名人故居': 'MRGJ', '清真寺': 'QZS', '景点的出入口': 'JDDCRK', '寺庙': 'SM', '道观': 'DG', '塔': 'T', '体育馆': 'TYG', '游泳': 'YY', '球类馆': 'QLG', '溜冰场': 'LBC', '滑雪场': 'HXC', '高尔夫球场': 'GEFQC', '其他运动健身': 'QTYDJS', '俱乐部': 'JLB', '加油加气站': 'JYJQZ', '充电站': 'CDZ', '汽车销售': 'QCXS', '汽车租赁': 'QCZL', '汽车用品': 'QCYP', '汽车维修': 'QCWX', '养护与美容': 'HYYMR', '洗车场': 'XCC', '机动车检测场': 'JDCJCC', '电子游戏厅及网吧': 'DZYXTJWB', 'KTV': 'KTV', '音乐厅': 'YYT', '娱乐城': 'YLC', '歌舞厅': 'GWT', '夜总会': 'YZH', '保龄球馆': 'BLQG', '洗浴中心': 'XYZX', '海滨浴场': 'HBYC', '电影院': 'DYY', '演出场所售票处': 'YCCSSPC', '剧场': 'JC', '休闲运动': 'XXYD', '棋牌室': 'QPS', '台球室': 'TQS', '美容美发': 'MRMF', '足疗': 'ZL', '按摩': 'AM', '其他休闲娱乐': 'QTXXYL', '非星级度假村': 'FXJDJC', '疗养院': 'LYY', '星级酒店': 'XJJD', '旅馆': 'LG', '招待所': 'ZDS', '经济型连锁酒店': 'JJXLSJD', '其他宾馆饭店': 'QTBGFD', '超市': 'CS', '品牌服饰专卖': 'PPFSZM', '孕婴幼用品专卖': 'YYYYPZM', '其他服装鞋帽': 'QTFZPM', '烟酒、饮料专卖': 'YJYLZM', '地方特产专卖': 'DDTCZM', '粮油食品': 'LYSP', '农副产品': 'NFCP', '水产市场': 'SCSC', '家居建材': 'JJJC', '图书': 'TS', '音像制品': 'YXZP', '电子商场': 'DZSC', '电器商场': 'DQSC', '商铺': 'SP', '钟表/眼镜': 'ZBYJ', '鲜花': 'XH', '珠宝': 'ZB', '工艺品及古玩字画': 'GYPJGWZH', '洗衣': 'XY', '便利店': 'BLD', '商业街': 'SYJ', '小商品市场': 'XSPSC', '中小学校': 'ZXXX', '大学院校': 'DXYX', '校内附属设施': 'YNFSSS', '科研院所': 'KYYS', '文化馆': 'WHG', '活动中心': 'HDZX', '文化宫': 'WHG', '幼儿园': 'YEY', '图书馆': 'TSG', '档案馆': 'DAG', '青少年宫': 'QSNG', '科技馆': 'KJG', '天文馆': 'TWG', '美术馆/画廊': 'MSGHL', '艺术团体': 'YSTT', '其他培训中心': 'QTPXZX', '技术类学校': 'JSLXX', '特殊教育': 'TSJY', '职业教育': 'ZYJY', '出国留学': 'CGLX', '艺术培训': 'YSPX', '继续教育': 'JXJY', '其他文化教育': 'QTWHJY', '地铁站': 'DTZ', '公交车站': 'GJCZ', '火车站': 'HCZ', '城市轨道交通出入口': 'CSGDJTCRK', '机场': 'JC', '机场附属设施': 'JCFSSS', '码头': 'MT', '渡口': 'DK', '高速公路出入口': 'GSGLCRK', '高速公路服务区': 'GSGLFWQ', '收费站': 'SFZ', '道路': 'DL', '立交桥': 'LJQ', '停车场': 'TCC', '路口': 'LK', '交通运输': 'JTYS', '长途客运站': 'CTKY', '桥梁': 'QL', '隧道': 'SD', '中国电信': 'ZGDX', '邮局': 'YJ', '邮政速递': 'YZSD', '其他邮电通信': 'QTYDTX', '中国移动': 'ZGYD', '中国联通': 'ZGLT', '社区医疗': 'SQYL', '药店': 'YD', '综合医院': 'ZHYY', '专科医院': 'ZKYY', '急救中心': 'JJZX', '康复中心': 'KFZX', '门诊/卫生所': 'MZWSS', '疾病防治': 'JBFZ', '疾病预防中心': 'JBYFZX', "家电数码": "JDSM"}

        self.type1_dict = {'餐饮服务': 'CYFW', '旅游景点': 'LYJD', '运动场馆': 'YDCG', '汽车服务': 'QCFW', '休闲娱乐': 'XXYL', '宾馆饭店': 'BGFD', '综合商场': 'ZHSC', '文化教育': 'WHJY', '交通设施': 'JTSS', '邮政电信': 'YZDX', '医疗卫生': 'YLWS'}

    def generic_shuffle(self, data):
        re_data = dict()
        serial_number = req_for_serial_number(code="MAPBAR")
        re_data["ID_"] = serial_number
        re_data["NAME_"] = data["NAME_"]
        re_data["ADDRESS_"] = data["ADDRESS_"].replace("|", "")
        re_data["ADDRESS_"] = re_data["ADDRESS_"].replace("地址：", "")
        # re_data["PROVINCE_CODE_"] = "3100"
        # re_data["PROVINCE_NAME_"] = "上海市"
        # re_data["CITY_CODE_"] = "310100"
        # re_data["CITY_NAME_"] = "上海市"
        re_data["HOT_"] = 0
        # 数据来源 URL
        source = re.findall(r"(https?://.*?)/", data["URL_"])
        re_data["SOURCE_"] = source[0]
        # 数据来源 网站名称
        re_data["SOURCE_NAME_"] = "图吧"
        re_data["SOURCE_TYPE_"] = "图吧"
        # 获取经纬度
        try:
            if re_data["ADDRESS_"]:
                location_result = get_lat_lng(address=re_data["ADDRESS_"])
                if location_result["status"] == 0:
                    re_data["LNG_"] = str(location_result["result"]["location"]["lng"])
                    re_data["LAT_"] = str(location_result["result"]["location"]["lat"])
                else:
                    re_data["LNG_"] = ""
                    re_data["LAT_"] = ""
                    self.logger.warning(f"_id: {data['_id']} 获取经纬度失败")
            else:
                re_data["LNG_"] = ""
                re_data["LAT_"] = ""
        except Exception as e:
            self.logger.exception(f"_id: {data['_id']} 获取经纬度失败, error: {e}")
        if re_data["LAT_"]:
            try:
                area_result = get_area(",".join([str(re_data["LAT_"]), str(re_data["LNG_"])]))
            except Exception as e:
                self.logger.exception(f"_id: {data['_id']} 获取地址失败, error: {e}")
            else:
                try:
                    re_data["AREA_NAME_"] = area_result["result"]["addressComponent"]["district"]
                except KeyError:
                    re_data["AREA_NAME_"] = ""
                try:
                    re_data["AREA_CODE_"] = area_result["result"]["addressComponent"]["adcode"]
                except KeyError:
                    re_data["AREA_CODE_"] = ""
                else:
                    re_data["CITY_CODE_"] = re_data["AREA_CODE_"][:4] + "00"
                    re_data["PROVINCE_CODE_"] = re_data["AREA_CODE_"][:2] + "00"
                    for city in self.city_list:
                        if city["CODE_"] == re_data["CITY_CODE_"]:
                            re_data["CITY_NAME_"] = city["NAME_"]
                            break
                    for prov in self.province_list:
                        if prov["CODE_"] == re_data["PROVINCE_CODE_"]:
                            re_data["PROVINCE_NAME_"] = prov["NAME_"]
                            break

        if not re_data.get("CITY_NAME_", ""):
            for city in self.city_list:
                if city["NAME_"][:2] in data["TYPE_"]:
                    re_data["CITY_CODE_"] = city["CODE_"]
                    re_data["CITY_NAME_"] = city["NAME_"]
                    break
            if re_data.get("CITY_NAME_", ""):
                for prov in self.province_list:
                    if prov["CODE_"][:2] == re_data["CITY_CODE_"][:2]:
                        re_data["PROVINCE_CODE_"] = prov["CODE_"]
                        re_data["PROVINCE_NAME_"] = prov["NAME_"]
                        break

        # CHA_BRANCH_MAIN_ROUTE 主干道
        if "道路" in data["TYPE_"]:
            road_data = dict()
            road_data.update(re_data)
            road_data["ID_"] = req_for_serial_number(code="WD_GD")
            road_data["ADDR_"] = road_data["ADDRESS_"]
            del road_data["ADDRESS_"]
            road_shuffle_data = super(MapbarScript, self).generic_shuffle(data=data, re_data=road_data, field=None)

        # CHA_BRANCH_FACILITY 图吧
        # serial_number = req_for_serial_number(code="MAPBAR")
        # re_data["ID_"] = serial_number
        re_data["TYPE1_"] = data["BTYPE_"]
        try:
            re_data["TYPE1_CODE_"] = self.type1_dict[re_data["TYPE1_"]]
        except KeyError:
            raise Exception("暂不需要清洗的数据")
        # 小分类清洗(合并部分分类)
        if data["TYPE_"][2:] in ["户外运动俱乐部", "赛马场及马术俱乐部", "室内运动健身俱乐部"]:
            re_data["TYPE2_"] = "俱乐部"
            re_data["TYPE2_CODE_"] = "JLB"
        elif data["TYPE_"][2:] in ["连锁店", "便利店"]:
            re_data["TYPE2_"] = "便利店"
            re_data["TYPE2_CODE_"] = "BLD"
        elif data["TYPE_"][2:] in ["电子商城", "电器商城"]:
            re_data["TYPE2_"] = "家电数码"
            re_data["TYPE2_CODE_"] = "JDSM"
        elif data["TYPE_"][2:] in ["诊所/卫生所", "门诊/急诊部"]:
            re_data["TYPE2_"] = "门诊/卫生所"
            re_data["TYPE2_CODE_"] = "MZWSS"
        else:
            re_data["TYPE2_"] = data["TYPE_"][2:]
            re_data["TYPE2_CODE_"] = self.type2_dict.get(re_data["TYPE2_"])
        re_data["SOURCE_TYPE1_"] = data["BTYPE_"]
        re_data["SOURCE_TYPE1_CODE_"] = self.type1_dict.get(re_data["SOURCE_TYPE1_"])
        re_data["SOURCE_TYPE2_"] = data["TYPE_"][2:]
        re_data["SOURCE_TYPE2_CODE_"] = self.source_type2_dict.get(re_data["SOURCE_TYPE2_"])
        re_data["PHONE_"] = data["PHONE_"].replace("无，", "")
        re_data["BUS_"] = data["BUS_"]
        re_data["BUSSTOP_"] = data["BUSSTOP_"]

        shuffle_data = super(MapbarScript, self).generic_shuffle(data=data, re_data=re_data, field=None)

        return_list = list()
        return_list.append({"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_FACILITY"), "DATA_": shuffle_data})
        if "road_shuffle_data" in dir():
            return_list.append({"TABLE_NAME_": TABLE_NAME("CHA_BRANCH_MAIN_ROUTE"), "DATA_": road_shuffle_data})
        return return_list


if __name__ == '__main__':
    # try:
    #     param = sys.argv[1]
    # except Exception:
    #     # param = "{'entityCode': 'MAPBAR_DEATAIL', 'limitNumber':2}"
    #     param = "{}"

    param = "{'entityType':'MAP_BAR','limitNumber':1,'entityCode':['MAPBAR_DEATAIL_BJ']}"

    # todo remove these code if  MongoDB collection is unified
    if "beijing" in param or "MAPBAR_DEATAIL_BJ" in param:
        collection = "mapbar_beijing"
    elif "shanghai" in param or "'MAPBAR_DEATAIL'" in param:
        collection = "mapbar_shanghai"
    else:
        collection = "mapbar"

    script = MapbarScript(table_name=TABLE_NAME("CHA_BRANCH_FACILITY"), collection_name=collection,
                          param=param, verify_field={"URL_": "URL_"})
    script.main()
