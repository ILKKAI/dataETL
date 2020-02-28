# -*- coding: utf-8 -*-
import base64
import time
import requests
import random
import http.client

# from SpidersLog.ICrwlerLog import ICrawlerLog
from log.data_log import Logger


class WanDou(object):

    def wandou(self):
        """
        豌豆代理获取
        :return:
        """
        # log = ICrawlerLog(name='spider').save
        log = Logger().logger
        url_wandou = r'http://h.wandouip.com/get/ip-list?pack=853&num=1&xy=1&type=2&lb=\r\n&mr=1&'
        try:
            time.sleep(random.randint(1, 5))
            re = requests.get(url=url_wandou).json()
            print(re)
            time.sleep(100)
        except:
            print(2)
            log.error('豌豆代理外部接口获取ip异常！')
            return False
        i = re.get('data')[0]
        ip = '{ip}:{port}'.format(ip=i.get('ip'), port=i.get('port'))
        print(ip)
        return ip

    def base_code(self, username, password):
        str = '%s:%s' % (username, password)
        encodestr = base64.b64encode(str.encode('utf-8'))
        return '%s' % encodestr.decode()

    def http_client(self, url, param=None, method='GET', code="utf-8"):
        # log = ICrawlerLog(name='spider').save
        log = Logger().logger
        # username = "499413642@qq.com"  # 您的用户名
        username = "123"  # 您的用户名
        # password = "***"  # 您的密码
        password = "123"  # 您的密码

        ip = self.wandou()
        ips = ip.split(':')
        proxy_ip = str(ips[0])  # 代理ip;
        proxy_port = str(ips[1])  # 代理端口号;
        print(proxy_ip, proxy_port)
        headers = {'Proxy-Authorization': 'Basic %s' % (self.base_code(username, password))}

        if param:
            headers = dict(headers, **param)
        try:
            con = http.client.HTTPConnection(proxy_ip, port=proxy_port, timeout=10)
            con.request(method, url, headers=headers)
            resu = con.getresponse()
            text = resu.read().decode(code, errors="ignore")
            return text
        except Exception as e:
            log.error(e.args)
            return None


if __name__ == '__main__':
    url = 'https://www.baidu.com/'
    WanDou().wandou()
    # print(WanDou().http_client(url=url))
