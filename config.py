#-*- coding:utf-8 -*-

'''爬虫控制器相关的配置信息
'''

import logging
import sys
import pymongo

#中央控制器地址和端口
#HOST = '0.0.0.0'
#PORT = 9001

#数据库地址和端口
DB_HOST = '219.224.135.60'
DB_PORT = 27017

#数据库账号信息
DB_USER = 'root'
DB_PWD = 'root'

#登录账号信息
WEIBO_USER = 'linhao1992@gmail.com'#'buaalinhao@163.com'
WEIBO_PWD = 'weibomap'#'weibomap'

#cookies文件
COOKIES_FILE = 'cookies.txt'

#GSID
GSID = 'gsid_CTandWM=4uuEa7521432jn9dWlcxL7EbNd5' #naive way

#设置日志记录对象
LOG_FILE = 'log.txt'
LOG_FORMAT = '%(levelname)s %(asctime)-15s %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOGGER = logging.getLogger()
fmt = logging.Formatter(LOG_FORMAT , DATE_FORMAT)
hdlr = logging.FileHandler(LOG_FILE, 'a')
hdlr.setFormatter(fmt)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)
print >> sys.stderr,'logging ok.'

def getDB():
    '''获取数据库对象
    '''
    connection = pymongo.Connection(DB_HOST, DB_PORT)
    db = connection.admin
    db.authenticate(DB_USER, DB_PWD)
    return connection.test_crawler_liwenwen