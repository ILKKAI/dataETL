# -*- coding: utf-8 -*-
import json

import requests
from retrying import retry

# api url
# URL = "http://api.map.baidu.com/geocoder/v2/"
URL = "http://api.map.baidu.com/geocoding/v3/"
# # kai
# AK = "GnZYZ8IczNld5GWIzzGdaz2Qjc32R3DP"
# ding
AK = "cPIEoR0QbIgCSdKzdQ9iaZn0eawAcd9D"
# 输出方式
OUTPUT = "json"


# @retry(stop_max_attempt_number=5)
def get_lat_lng(address):
    """
    根据地址获取经纬度
    :param address: 地址
    :return:
    """
    address = address.replace("#", "号")
    # url = URL + "?" + "address=" + address + "&output=" + OUTPUT + "&ak=" + AK
    url = f"http://api.map.baidu.com/geocoding/v3/?address={address}&output={OUTPUT}&ak={AK}"
    # url = f"http://api.map.baidu.com/geocoding/v3/?address={address}&output={OUTPUT}&ak={AK}&callback=showLocation"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    if "msg" in temp:
        if "address length too long" in temp["msg"]:
            return get_lat_lng(address[:-5])
        else:
            return temp
    else:
        return temp


@retry(stop_max_attempt_number=5)
def get_area(lat_lng):
    """
    根据经纬度获取地址
    :param lat_lng: 经纬度
    :return:
    """
    # url = URL + "?location={}&output={}&pois=1&latest_admin=1&ak={}".format(lat_lng, OUTPUT, AK)
    url = f"http://api.map.baidu.com/reverse_geocoding/v3/?ak={AK}&output={OUTPUT}&coordtype=wgs84ll&location={lat_lng}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_periphery(classify, tag, lat_lng, radius, page_num, coord_type=3):
    """
    根据经纬度获取周边
    :param classify: (银行)
    :param tag: (金融)
    :param lat_lng: 经纬度
    :param radius: 半径(米)
    :param page_num: 页数
    :param coord_type: 坐标类型
    :return:
    """
    #  &coord_type=1
    url = f"http://api.map.baidu.com/place/v2/search?" \
        f"query={classify}&tag={tag}&location={lat_lng}&page_size=20&radius={radius}&" \
        f"output=json&ak={AK}&page_num={page_num}&coord_type={coord_type}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_infomation(uid):
    url = f"http://api.map.baidu.com/place/v2/detail?uid={uid}&output=json&scope=2&ak={AK}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_infomation(query, city):
    url = f"http://api.map.baidu.com/place/v2/suggestion?query={query}&region={city}&city_limit=true&output=json&ak={AK}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


from math import radians, cos, sin, asin, sqrt


def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    计算两点之间的大圆距离,在地球上(以十进度表示)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r


def addr_(addr=None):
    return get_lat_lng(addr).get('result').get('location').get('lng')


def addr_2(addr=None):
    return get_lat_lng(addr).get('result').get('location').get('lat')


if __name__ == '__main__':
    # print(get_lat_lng('澳门'))
    print(get_lat_lng('上海市徐汇区漕溪北路18号上实大厦1楼(虹桥路)'))

    # from urllib.parse import urlparse
    # print(hash(urlparse('https://zhuanlan.zhihu.com/p/33921883').netloc))
    # print(urlparse('https://zhuanlan.zhihu.com/p/33921883').netloc)

    # import pandas
    # data = pandas.read_excel(r'C:\Users\xiaozhi\Desktop\浦发招行地图需要数据.xlsx', header=0, sheet_name='Sheet1')
    # data['lng'] = data['地址'].apply(addr_)
    # data['lat'] = data['地址'].apply(addr_2)
    # data.to_excel(r'C:\Users\xiaozhi\Desktop\浦发招行地图需要数据.xlsx')

    # print(get_periphery(classify="银行", tag="金融", lat_lng=[113.33968495640754, 23.12695153593457], radius=3000, page_num=1))
    # print(get_lat_lng('广州市天河区天河路621-625号'))

    # print(haversine(104.07439296975636, 30.637740057562045, 104.07075653922142, 30.66258246945587))
    # lat_result = get_lat_lng("北京市西城区太平桥大街18号丰融国际大厦(原址东四十条68号平安发展大厦1层,2010年7月19日迁入)")
    # print(lat_result)
    # print(type(lat_result))
    # result = get_area(",".join([str(lat_result["result"]["location"]["lat"]), str(lat_result["result"]["location"]["lng"])]))
    # print(result)
    # quit()
    #
    # # # 获取周边
    # station = "安定镇于家务"
    # i = 0
    # while True:
    #     x3 = get_periphery(classify="公交车站", tag="交通设施", lat_lng="39.62069758500963,116.56424983967442", radius=3000, page_num=i)
    #     # print(x3)
    #     for nearby in x3["results"]:
    #         if station in nearby["name"]:
    #             print(nearby)
    #             break
    #     i += 1
    #     if len(x3["results"]) != 20:
    #         break


