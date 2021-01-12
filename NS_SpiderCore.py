import re
import socks
import socket
import sqlite3
import hashlib
import logging
import requests

from bs4 import BeautifulSoup
from urllib.parse import urlparse
from NS_Utils import initGraph, modifyUrlBySide, digestPage


class Spider(object):

    def __init__(self, url: str, logger, timeout=20, proxy_list=[]):
        # 初始化Spider类，URL是爬虫目标链接，timeout是请求时延上限，logger是日志句柄，proxy_list是代理库，flags用于标记爬虫预处理过程中各项值
        self.URL = url
        self.timeout = timeout
        self.logger = logger
        self.proxy_list = proxy_list
        self.flags = {}

    def takeAction(self, dbconn: sqlite3.Connection):

        def prepare_url_text():
            # 矫正并检查爬虫目标链接文本上的合法性
            self.flags["url_text_valid"] = True
            if self.URL_checker() is None:
                self.URL_formatter()
                self.logger.critical("Url:{url} invalid".format(url=self.URL))
                if self.URL_checker() is None:
                    self.logger.critical("Url:{url} invalid after formatter".format(url=self.URL))
                    self.flags["url_text_valid"] = False
                else:
                    self.logger.info("Url:{url} valid".format(url=self.URL))
            else:
                self.logger.info("Url:{url} valid".format(url=self.URL))

        def prepare_url_conn():
            # 检查爬虫目标链接是否可达
            self.URL_extractor()
            self.flags["url_conn_avail"] = False
            for _ in range(4):
                self.URL_connector()
                if self.flags["url_conn_avail_by_self"] is True or self.flags["url_conn_avail_by_proxy"] is True:
                    self.flags["url_conn_avail"] = True
                    break

        prepare_url_text()
        if self.flags["url_text_valid"] is True:
            prepare_url_conn()
            if self.flags["url_conn_avail"] is True:
                # 若一切正常，则开始爬取
                spiderAction = SpiderAction(self.URL, self.logger, dbconn, self.timeout, self.proxy_list)
                return spiderAction.onceSpiderNode()

    def URL_extractor(self):
        # 通过urlparse提取爬虫目标连接中的PROTOCOL、IP和PORT
        pr = urlparse(self.URL)
        self.flags["SSL"] = True if pr.scheme == 'https' else False
        self.flags["NETLOC"] = {}
        self.flags["NETLOC"]["HOST"] = pr.netloc.split(':')[0]
        if len(pr.netloc.split(':')) == 1:
            # flags内设置默认端口
            self.flags["NETLOC"]["PORT"] = 443 if self.flags["SSL"] is True else 80
        else:
            # flags内设置URL内指定端口
            self.flags["NETLOC"]["PORT"] = int(pr.netloc.split(':')[1])

    def URL_connector(self):
        # 通过socket测试爬虫目标链接的可达性
        sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cr = sockfd.connect_ex((self.flags["NETLOC"]["HOST"], self.flags["NETLOC"]["PORT"]))
        self.flags["url_conn_avail_by_self"] = False
        if cr == 0:
            # 直连成功
            self.logger.info("Url:{url} has been connected by socket without socks proxy".format(url=self.URL))
            self.flags["url_conn_avail_by_self"] = True
        else:
            # 直连失败
            self.logger.critical("Url:{url}'s not connected by socket without socks proxy".format(url=self.URL))
            self.logger.info("Try to connect to Url:{url} by socks socket".format(url=self.URL))
            self.flags["url_conn_avail_by_proxy"] = False
            if len(self.proxy_list) > 0:
                # 尝试使用代理库内代理连接
                for proxy in self.proxy_list:
                    # 轮流尝试代理库资源
                    ppr = urlparse(list(proxy.values())[0])
                    socks.set_default_proxy(proxy_type=socks.HTTP, addr=ppr.hostname, port=ppr.port)
                    socket.socket = socks.socksocket
                    sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sockfd.settimeout(self.timeout)
                    cr = sockfd.connect_ex((self.flags["NETLOC"]["HOST"], self.flags["NETLOC"]["PORT"]))
                    sockfd.settimeout(None)
                    if cr == 0:
                        # 代理直连成功
                        self.logger.info(
                            "Url:{url}'s connected by socket with socks proxy {proxy_url}"
                                .format(url=self.URL, proxy_url=ppr.netloc))
                        self.flags["url_conn_avail_by_proxy"] = True
                        self.flags["live_proxy"] = proxy
                        break
                    else:
                        # 代理直连失败
                        self.logger.critical(
                            "Url:{url}'s not connected by socket with socks proxy {proxy_url}"
                                .format(url=self.URL, proxy_url=ppr.netloc))
            else:
                self.logger.warning("no proxy available")

    def URL_formatter(self):
        # 去除爬虫目标链接的非法字符
        self.URL = self.URL \
            .replace(' ', '') \
            .replace('\t', '') \
            .replace('\r', '') \
            .replace('\n', '')

    def URL_checker(self):
        # 通过正则检查爬虫目标链接是否合法
        checker = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return checker.fullmatch(self.URL)


class SpiderAction(object):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/56.0.2924.87 Safari/537.36 '
    }

    def __init__(self, url: str,
                 logger: logging.Logger,
                 dbconn: sqlite3.Connection,
                 timeout=20,
                 proxy_list=[]):
        self.URL = url
        self.logger = logger
        self.dbconn = dbconn
        self.timeout = timeout
        self.proxy_list = proxy_list
        self.flags = {"SSL": True if urlparse(self.URL).scheme == 'https' else False}
        initGraph(self.logger, self.dbconn, self.URL)

    def onceSpiderNode(self):
        r = requests.get(self.URL, headers=self.headers, verify=False)
        soup = BeautifulSoup(r.content, 'html.parser')
        link_list = set([link.get('href') for link in soup.find_all('a')])
        return_list = []
        for link in link_list:
            if link is not None:
                root = self.URL
                tmp = modifyUrlBySide(root, link)
                if tmp["url_type"] == 2: break
                self.dbconn.cursor().execute('''INSERT OR IGNORE INTO '{tbname}' (SUBURI, LOPCNT, LOCATE)
                    VALUES ('{suburi}', {lopcnt}, {locate})
                ;'''.format(tbname=hashlib.sha256(self.URL.encode('latin-1')).hexdigest(),
                            suburi=tmp["url"],
                            lopcnt=0,
                            locate=tmp["url_type"]))
                self.dbconn.commit()
                tmp["tags"] = digestPage(tmp["url"])
                # print(tmp)
                self.logger.info(str(tmp))
                return_list.append(tmp)
        self.logger.info("Spider once towards {url}".format(url=self.URL))
        print(return_list)
        return return_list

    # def recursionSpiderNodes(self):
    #     root_list = self.onceSpiderNode()
    #     for elem in root_list
    #     return
