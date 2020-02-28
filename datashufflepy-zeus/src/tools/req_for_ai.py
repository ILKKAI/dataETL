# -*- coding: utf-8 -*-
from math import radians, cos, sin, asin, sqrt
import requests
from retrying import retry

# from __config import *
# sys.path.append(AI_PATH)
# from SCRIPT.pipe import ModelPipe


# def model_pipe(func):
#     def inner_wrapper(*args, **kwargs):
#         modelCode, versionCode, params = func(*args, **kwargs)
#         results = ModelPipe(modelCode, versionCode, params).begin()
#         return results
#     return inner_wrapper


# modelCode = "LOCDUPLICATE"
# versionCode = "1.0"
# params = '{"text": "我爱北京天安门","POI":"[\'安博教育(育新大厦西北)\', \'榜样教育(万柳东路)\']"}'
#
# results = ModelPipe(modelCode, versionCode, params).begin()
# print(11, results)
hostPort_dev = "172.22.69.44:9999"
# hostPort_dev = "172.22.69.39:8098"
hostPort_pro = "192.168.1.12:8081"
# hostPort = hostPort_pro
hostPort = hostPort_dev


def highest_prob_entity(entityRec):
    """
    :param entityRec:实体识别模型返回结果
    :return:
    """
    if entityRec != {} and 'Organ' in entityRec:
        entity = entityRec.get('Organ').get('entity').strip().split(' ')
        # print("entity:",entity)
        entitySet = list(set(entity))
        # print("entitySet:", entitySet)
        itemCount = dict()
        for item in entitySet:
            itemCount[item] = entity.count(item)
        # print(itemCount)
        sortedEntity = sorted(itemCount.items(), key=lambda x: x[1], reverse=True)
        # print(sortedEntity[0][0])
        entityRec['Organ']['entity'] = sortedEntity[0][0]
        # print(entityRec)
    return entityRec


# @model_pipe
def req_for_ner(text):
    """
    识别文本中的命名实体，包括人名，地理位置，机构，时间等。
    :param text:
    :return:{
        'Organ': {
            'tag': 'ORG',
            'name': '机构',
            'entity': ' 桂林银行 桂林银行梧州分行 梧州银保监分局'
        },
        'Time': {
            'tag': 'TM',
            'name': '时间',
            'entity:': ' 年初 2月11日'
        },
        'Location': {
            'tag': 'LOC',
            'name': '地点',
            'entity': ' 成都市 锦江区'
        }
    }
    """

    url = "http://" + hostPort + "/ner"
    # url = "http://onestopdata.pactera.com:9999/ner"
    response = requests.post(url=url, data={"text": text})
    response.close()
    # print(response.content.decode("utf-8"))
    # print(111, response.text)
    if not response.text:
        return
    else:
        result = highest_prob_entity(response.json())
        return result

    # modelCode = "NER"
    # versionCode = NER_VERSION
    # params = f'{{"text": {text}}}'
    # return modelCode, versionCode, params


def req_for_comment(text):
    """
    将微博评论分类，积极、中性和敏感。
    :param text:
    :return:{'sentiment':'正面'}
    """

    url = "http://" + hostPort + "/sinaweibocomment"
    # url = "http://" + hostPort + "/comment"
    response = requests.post(url=url, data={"text": text})
    response.close()
    if not response.text:
        return
    else:
        return response.json()


def req_for_senti(text):
    """
    对新闻资讯(标题)正负面进行识别，包括正面/敏感/中性。
    :param text:
    :return:{'sentiment': '正面'}
    """

    url = "http://" + hostPort + "/sentiment"
    response = requests.get(url=url, params={"title": text})
    response.close()
    if not response.text:
        return
    else:
        return response.json()


@retry(stop_max_attempt_number=5)
def req_for_ts(text):
    """
    提取文本摘要，摘要长度与文本长度成正比。
    :param text:
    :return:{'summary': '据有关资料显示，北斗卫星导航系统是中国自主建设独立运行，并与世界其他卫星导航系统兼容共用的全球卫星导航系统。'}
    """

    url = "http://" + hostPort + "/textrank"
    response = requests.post(url=url, data={"text": text})
    response.close()
    if not response.text:
        return
    else:
        return response.json()


def req_for_censor(text):
    """
    检测资讯是否含有色情/反动/含有不雅词汇等。
    :param text:
    :return:{
        'censor': '该文本含有敏感信息，包括色情/反动/不雅词汇等',
        'text': '达赖喇嘛在印度国会发表演讲'
    }
    """

    url = "http://" + hostPort + "/censor"
    response = requests.post(url=url, data={"text": text})
    response.close()
    if not response.text:
        return
    else:
        return response.json()


def req_for_textLoc(text):
    """
    计算文本的地理维度，即最大概率属于哪个地址
    :param text:
    :return:{
        "addr"  :"四川省成都市中国西部国际博览城",
        "tagsId":  "xxxxxx"
    }
    """
    data = {"text": text}
    url = "http://" + hostPort + "/textlocation"
    response = requests.post(url=url, data=data)
    response.close()
    if not response.text:
        return
    else:
        return response.json()


def req_for_poi(text):
    """
    poi范围内机构名称去重
    :param text: dict: {"type": "上海大保健", "name": ["dabaojian1", "dabaojian2"]}
    :return:{
        "type"  : "上海连锁店",
        "count":  "100"
    }
    """
    url = "http://" + hostPort + "/duplicate"
    response = requests.get(url=url, params={"poi": text})
    response.close()
    if not response.text:
        return
    else:
        return response.json()


def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    根据经纬度测算直线距离
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    # return c * r * 1000
    return c * r


def req_for_weibo_hot(publish_time, relays, replies, praises, verifieds):
    """
    微博热度
    :param publish_time:
    :param relays:
    :param replies:
    :param praises:
    :param verifieds:
    :return:
    """
    url = "http://" + hostPort + "/hotnumber"
    response = requests.get(url=url,
                            params={
                                "publish_time": publish_time, "relays": relays, "replies": replies, "praises": praises,
                                "verifieds": verifieds
                            })
    response.close()
    if not response.text:
        return
    else:
        # print(response.content.decode("utf-8"))
        return response.json()


def req_for_news_hot(title, content):
    """
    资讯热度
    :param title:
    :param content:
    :return:
    """
    url = "http://" + hostPort + "/bdlabel"
    response = requests.post(url=url, data={"title": title, "content": content})
    response.close()
    if not response.text:
        return
    else:
        # print(response.content.decode("utf-8"))
        return response.json()


def req_for_credit_relative(text, target_word="信用卡"):
    """
    识别资讯是否与信用卡关联，并提取出关联词
    :param text:
    :param target_word:
    :return:
    """
    url = "http://" + hostPort + "/creditsimiwords"

    response = requests.post(url=url, data={"text": text, "targetword": target_word})
    response.close()

    if not response.text:
        return
    else:
        # print(response.content.decode("utf-8"))
        return response.json()


if __name__ == '__main__':
    a = req_for_ts("原标题：光大银行换帅！新行长是个70后管理学博士作者：彭明空缺了近3个月之后，光大银行迎来了新一任行长。11月12日上午，光大集团召开内部会议，宣布光大集团副总葛海蛟将出任光大银行行长一职，其任职资格尚待银保监会批复。资料显示，葛海蛟此前曾在农业银行（3.610，-0.03，-0.82%）任职多年，具备丰富的商业银行从业经验。光大银行迎新行长葛海蛟光大银行送走张金良，迎来葛海蛟。葛海蛟来自光大集团，从简历来看，年富力强，具备丰富的银行从业经验或许可以简单概括这位光大银行新任行长。根据光大集团官网介绍，葛海蛟，现任中国光大集团股份公司党委委员、副总经理。葛海蛟在光大集团主要分管证券、信托板块。")
    print(a)

    # print(a["Organ"]["entity"])
    # print(type(a["creditrelative"]))
    # quit()
    # print(req_for_comment("四川省成都市中国西部国际博览城"))
    # print(req_for_ner("原标题：光大银行换帅！新行长是个70后管理学博士作者：彭明空缺了近3个月之后，光大银行迎来了新一任行长。11月12日上午，光大集团召开内部会议，宣布光大集团副总葛海蛟将出任光大银行行长一职，其任职资格尚待银保监会批复。资料显示，葛海蛟此前曾在农业银行（3.610，-0.03，-0.82%）任职多年，具备丰富的商业银行从业经验。光大银行迎新行长葛海蛟光大银行送走张金良，迎来葛海蛟。葛海蛟来自光大集团，从简历来看，年富力强，具备丰富的银行从业经验或许可以简单概括这位光大银行新任行长。根据光大集团官网介绍，葛海蛟，现任中国光大集团股份公司党委委员、副总经理。葛海蛟在光大集团主要分管证券、信托板块。此前，葛海蛟还曾担任光大证券（9.820，-0.15，-1.50%）董事一职。2017年3月30日上海证监局核准葛海蛟的光大证券董事任职资格。但在今年6月21日，光大证券公布，葛海蛟因工作原因，不再担任公司董事职务，其亦不再担任董事会薪酬、提名与资格审查委员会委员及战略及发展委员会召集人。从公开活动看，葛海蛟是在2016年底跳槽光大集团，而在此前，葛海蛟在农业银行任职多年，在2016年2月，其公开职务是农业银行国际业务部副总经理。银行从业经验丰富这位70后新行长，熟悉商业银行基本业务，有海外学习和工作经理，具有双语工作能力，精通国际业务、信贷客户业务和金融市场业务等。在跳槽光大集团之前，葛海蛟曾任中国农业银行辽宁省分行国际业务部副总经理、总经理，中国农业银行辽宁省辽阳市分行行长、党委书记，中国农业银行大连市分行副行长、党委委员，中国农业银行新加坡分行总经理，中国农业银行国际业务部副总经理（总经理级）兼任中国农业银行悉尼分行执行董事长（SOOA），中国农业银行黑龙江省分行行长、党委书记，黑龙江省第十二届人大代表。出生于1971年的葛海蛟教育背景也较为全面，经济金融基本理论功底扎实，拥有经济学、法学、管理学和金融学多学位，教育背景持续完整。资料显示，葛海蛟拥有辽宁大学法学和经济学双学士学位、吉林大学经济学硕士、南京农业大学管理学硕士学位、南京农业大学农业经济专业博士学位，伦敦商学院访问学者。前任行长跳槽中国邮政今年8月22日，光大银行执行董事、行长张金良因工作调整，向该行董事会提交辞呈，辞去执行董事、董事会普惠金融发展和消费者权益保护委员会主任委员及委员、战略委员会委员、风险管理委员会委员及行长职务。据悉，目前张金良已经到中国邮政集团（以下简称“邮政”）任总经理。资料显示，张金良具备丰富的银行从业经验。2016年1月担任中国光大集团执行董事、党委委员，2016年8月，任光大银行执行董事、行长、党委副书记职位。张金良接任之际，光大银行正面临经营转型、领导班子的新老交替以及金融改革等问题。在张金良调任至光大银行之前，光大银行的的净利润为295.28亿，总资产为3.17亿，而在张金良担任光大银行行长的第二年底，光大银行的净利润为315.5亿，同比增长4.02%，总资产为4.089亿。张金良的经营工作获得了光大银行的高度评价，称其自2016年1月加入本行以来，恪尽职守，勤勉履职，开拓创新，在组织和推动本行实施战略转型、调整业务结构、强化资本管理、发展普惠金融、加强风险防范、提升综合竞争力等方面发挥了重要作用。有消息称，此前光大银行董事长李小鹏甚至亲自送张金良到中国邮政集团赴任。光大银行资产排名11位在业内人士看来，在葛海蛟之前的两任行长均颇为专业，为光大银行的发展打下了比较好的基础，从近期的业绩来看，光大银行也表现出了比较好的发展潜力。资料显示，光大银行是光大集团旗下最重要的金融牌照，在12家股份制银行里不以规模取胜，但近年来综合业绩表现不错。境内设立分支机构已达到1196家，机构网点辐射全国129个经济中心城市。从三季报来看，公司2018年前三季度实现营业收入813.55亿元，同比增长18.26%；净利润277.98亿元，同比增长9.10%。基本每股收益0.50元。报告期末，不良贷款率1.58%，比上年末下降0.01个百分点；拨备覆盖率172.65%，比上年末上升14.47个百分点。股东方面，证金公司持股由二季度末的3.93%降至2.95%。在28家上市银行三季度的业绩中，光大银行归母净利润排名第11位。营收排名第12位，排名并无变动。利息收入增加67.06亿，手续费及佣金收入增加46.47亿，其他经营性净收益增加94.89亿。资产总计43554.29亿元，排名第11位。有银行业内人士表示，从其资产增速、存款增速，不良率和拨备覆盖率来看，在不考虑规模因素影响下，光大表现出较强的潜在竞争力。“一视同仁”助力民营经济今年，光大银行将普惠金融提升到前所未有的战略高度。在支持民营企业和民营经济方面，提出四个“一视同仁”支持民营经济。近日光大银行召开座谈会，光大集团、光大银行董事长李晓鹏在会上强调“光大银行一定要认真落实中央的指示精神，毫不动摇地支持民营经济发展”，支持民营经济要做到四个“一视同仁”，即信贷政策上一视同仁，不提高准入门槛；服务效率上一视同仁，不优柔寡断；激励约束上一视同仁，不厚此薄彼；产品创新上一视同仁，不左顾右盼。据悉，光大银行下一步将从六个“落实到位”着力，提高服务小微企业、民营企业的实效。一是专营机构建设落实到位。计划每家分行至少设立一家专营机构，重点开展单户授信金额在1000万元（含）以下的以民营企业为主的小微贷款业务；二是信贷工厂推广落实到位。年底前由现有的12家扩展为25家；三是产品服务创新落实到位。围绕产业链业务场景，进一步挖掘交易数据内涵价值，以信息科技为支撑，持续打造供应链融资竞争优势。稳步推进信用类贷款、无还本续贷等业务开展，优化扩展“光大快贷”线上系列产品，积极开展与国家融资担保基金合作；四是企业减免收费落实到位。严格执行“七不准”“四公开”，落实监管有关收费政策，在2017年减费金额近5亿元的基础上，继续主动免除资信证明费、银行承兑汇票敞口风险管理费等全部涉及小微企业的各项收费；五是资源配置保障落实到位。继续单列专项信贷额度，在资源有限的情况下，优先考虑小微企业、民营企业贷款净增投放；六是内部考核激励落实到位。提高普惠金融贷款在分行机构的考核占比、EVA绩效系数，对新投放普惠贷款给予内部价格优惠，落实尽职免责政策等。责任编辑：张译文"))
    # print(req_for_comment("我的中银余额理财， 25日上午6点多赎回，但是现在都还没到账，非常着急用钱，求助"))
    # # print(req_for_senti("江西省上饶市弋阳县方志敏大道149号"))
    # print(req_for_ts("善用资金自动归集让财富自发生长更新日期作者广发银行成功的财富管理靠的是什么不是运气也不是所谓内幕消息曾被美国纽约时报评为全球十大顶尖基金经理人的约翰内夫总结了被投资界普遍认同的三个投资成功原则纪律耐心和合理资产组合广发银行财富管理顾问指出资产组合以及分批持续投资原理对所有个人投资者都适用个人投资者应该综合自己的现金流收支情况投资风险承受能力理财目标等综合考虑设计一个适合自己的资产组合和持续投资计划并通过如基金定投黄金定投定期购入资产组合内圈定的投资产品等方式实现这么做不仅能有效的分散风险还能通过有计划的持续积累成功实现中长期的财富管理目标但零散的资金账户银行间财富管理服务的差异等因素让个人投资者的理财过程变得费时费力一般家庭要实现整体家庭财富管理似乎非常困难新婚的小吴就遇到了这个难题她和丈夫刚买了婚房此前的积蓄也用得差不多了本想把每月的收入节余进行合理投资积攥两年后生孩子以及日后孩子的教育经费但当她面对着家里一大堆的银行卡就犯了愁两夫妻工资卡分属两家银行房贷和水电费交纳的又是另外两家银行每月还得给双方父母的账户上打点钱孝敬孝敬更别提两人拥有的三张信用卡要还款的问题了就是要弄清楚自己每个月到底剩下多少钱可以投资都已经让小吴头痛不已这个情况非常普遍广发银行从客户需求出发整合我行先进电子银行渠道功能以及综合财富管理服务资源推出了资金自动归集功能并推出了一系"))
    # print(req_for_ts("•中国建设银行印发《消费者权益保护工作考核评价办法》"))
    # print(req_for_ner("江西省上饶市弋阳县方志敏大道149号"))
