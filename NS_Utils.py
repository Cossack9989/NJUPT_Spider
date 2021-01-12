import os
import time
import jieba
import jieba.analyse
import requests
import logging
import sqlite3
import hashlib
from bs4 import BeautifulSoup


def initLog():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
    log_path = os.path.dirname(os.getcwd() + '\\logs\\')
    log_name = log_path + '\\' + rq + '.log'
    logfileHandler = logging.FileHandler(log_name, mode='w')
    logfileHandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    logfileHandler.setFormatter(formatter)
    logger.addHandler(logfileHandler)
    return logger


def initDb(logger: logging.Logger):
    # 数据库初始化，connect的功能是不存在指定数据库则创建并连接，存在指定数据库则直接连接
    connector = sqlite3.connect('root.db')
    # 打印数据库创建成功的日志
    logger.info("root database connected")
    return connector


def initGraph(logger: logging.Logger, connector: sqlite3.Connection, root_url: str):
    # 打开数据库连接
    cursor = connector.cursor()
    # 查询数据库内全部表名
    tbs = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tbs_list = tbs.fetchall()
    logger.info('\n' + ''.join([tbname[0] + '\n' for tbname in tbs_list]))
    # 检查当前爬取URL的哈希是否与已有表名重合
    if (hashlib.sha256(root_url.encode('latin-1')).hexdigest(),) not in tbs_list:
        # 无重合则以当前爬取URL的哈希为名创建新表
        # SUBURI->爬取到的域名/链接
        # LOPCNT 暂无作用
        # LOCATE->链接是内部链接还是外部链接？
        cursor.execute('''CREATE TABLE '{URL}'(
                SUBURI TEXT PRIMARY KEY NOT NULL, 
                LOPCNT INTEGER,
                LOCATE INTEGER
            );'''.format(URL=hashlib.sha256(root_url.encode('latin-1')).hexdigest()))
        # 打印数据库新表创建成功的日志
        logger.info("graph of {url} inited".format(url=root_url))
        connector.commit()
    cursor.close()


def stopDb(logger: logging.Logger, connector: sqlite3.Connection):
    # 关闭数据库连接
    connector.close()
    # 打印数据库关闭到日志
    logger.info("root database closed")


def modifyUrlBySide(url: str, suburl: str):
    ###############################################################
    # url_type: 0: 内部URL
    #           1: 外部URL
    #           2: IPC
    ###############################################################
    tmp = {}
    # 判断是否有https://或http://或//开头的且与原URL不重复的外部链接
    if (suburl.startswith("http://") is True or \
        suburl.startswith("https://") is True or \
        suburl.startswith("//") is True) and \
            suburl.startswith(url) is False:
        tmp["url_type"] = 1
        # 将//开头的外部链接补全为http://开头的链接
        if suburl.startswith("//") is True:
            tmp["url"] = "http:" + suburl
        else:
            tmp["url"] = suburl
    # 判断是否有mailto或javascript开头的IPC链接
    elif suburl.startswith("mailto:") is True or \
            suburl.startswith("javascript:") is True:
        tmp["url_type"] = 2
    # 剩下的都是内部链接
    else:
        tmp["url_type"] = 0
        if suburl.startswith(url) is False:
            tmp["url"] = url + suburl
        else:
            tmp["url"] = suburl
    return tmp


def digestPage(url:str):
    ###############################################################
    # 网页关键词提取
    ###############################################################
    r = requests.get(url)
    # 用BeautifulSoup的轮子解析html
    soup = BeautifulSoup(r.content, 'html.parser')
    # 用jieba的轮子分析出网站关键词
    tags = jieba.analyse.extract_tags(soup.get_text(), topK=8, allowPOS=['n', 'nr', 'ns', 'nt', 'nw', 'nz', 'an', 'PER'])
    return tags


def formatTable(result: list):
    # 将爬虫返回值（列表嵌套字典）转化为HTML表格
    r = "<body style=\"background-color:black;\">\r\n"
    r += "<p style=\"font-size:50px; color: white; width:50%; left:45%; position:relative;\">RESULT</p>\r\n"
    #表格
    r += "<table class=\"dataframe\" border=\"0\"  style=\"color:white; position:absolute;top: 100px; margin-left:10%; margin-right:10%; \" >\r\n"
    #表格头部
    r += "\t<thead>\r\n"
    r += "\t\t<tr style=\"height: 40px; margin: 20px; font-size: 18px; font-weight: lighter;\">\r\n"
    r += "\t\t\t<th style=\"text-align: center;\">链接类型</th>\r\n"
    r += "\t\t\t<th style=\"text-align: center;\">链接</th>\r\n"
    r += "\t\t\t<th style=\"text-align: center;\">关键词</th>\r\n"
    r += "\t\t</tr>\r\n"
    r += "\t</thead>\r\n"
    #表格主体
    r += "\t<tbody>\r\n"
    #获取到返回值后根据格式自动填充表格
    for elem in result:
        r += "\t\t<tr style = \"height: 40px; font-size: 15px; font-weight: thin;\">\r\n"
        r += "\t\t\t<th style=\"text-align: center;\">{type}</th>\r\n".format(type="内部链接" if elem["url_type"] == 0 else "外部链接")
        r += "\t\t\t<th style=\"text-align: center;\">{link}</th>\r\n".format(link=elem["url"])
        r += "\t\t\t<th style=\"text-align: center;\">{tags}</th>\r\n".format(tags=' '.join(elem["tags"]))
        r += "\t\t</tr>\r\n"
    r += "\t</tbody>\r\n"
    r += "</table>"
    r +="</body>"
    return r