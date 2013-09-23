# -*- coding: utf-8 -*-


import re
import time
import threading
import codecs
import Queue
import sys
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from config import WEIBO_USER, WEIBO_PWD, getDB
from getpass import getpass
from selenium.common.exceptions import TimeoutException, WebDriverException
import pickle

db = getDB()

def update_name_password(WEIBO_USER, WEIBO_PWD):
    db.authusers.update({'name': WEIBO_USER}, {'$set': {'pwd': WEIBO_PWD}}, True)

class Spider(object):

    def __init__(self, WEIBO_USER, WEIBO_PWD):
        self.spiders = []
        #self.client = webdriver.Remote(command_executor="http://219.224.135.60:4444/wd/hub", desired_capabilities={'browserName': 'firefox', 'platform': 'ANY'})
        self.client = webdriver.Chrome()
        login_url = 'http://login.weibo.cn/login/?ns=1&revalid=2&backURL=http%3A%2F%2Fweibo.cn%2F&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt='
        self.client.get(login_url)
        input_elements = self.client.find_elements_by_tag_name('input')
        mobile_input_element = self.client.find_element_by_name("mobile")
        submit_input_element = self.client.find_element_by_name("submit")
        pwd_input_element = input_elements[1]
        mobile_input_element.send_keys(WEIBO_USER)
        pwd_input_element.send_keys(WEIBO_PWD)
        submit_input_element.click()
        time.sleep(5)
        home_page_soup = BeautifulSoup(self.client.page_source)
        if home_page_soup:
            me_soup = home_page_soup.find('div', {'class': 'me'})
            c_soup = home_page_soup.find('div', {'class': 'c'})
            if me_soup and me_soup.string.encode('utf-8') == '登录名或密码错误':
                self.client.quit()
                sys.exit('login authentication failed, please check your name or pwd and retry.')
            elif c_soup and c_soup.contents[0] and c_soup.contents[0] == u'登':
                self.client.quit()
                sys.exit('login authentication failed, please check your name or pwd and retry.')    
            else:
                print  'success login'
                update_name_password(WEIBO_USER, WEIBO_PWD)
        cookies = self.client.get_cookies()[0]
        cookies.update({'_id': int(cookies['expiry'])-30*24*3600})
        print 'cookies: ', cookies
        db.cookies.save(cookies)

    def spider(self, start_page, uid_queue, total_uids, join=False, end_page=100):
        st = SpiderThread(1, start_page, end_page, uid_queue, total_uids, controler=self, client=self.client)
        st.setDaemon(True)
        st.start()
        self.spiders.append(st)
        if join:
            st.join()

class SpiderThread(threading.Thread):
    def __init__(self, num, start_page, end_page, uid_queue, total_uids, controler=None, client=None):
        self.num = num
        self.controler = controler
        self.client = client
        self.start_page = start_page
        self.end_page = end_page
        self.uid_queue = uid_queue
        self.total_uids = total_uids
        threading.Thread.__init__(self)
    
    def run(self):##重写run方法
        while not self.uid_queue.empty():
            uid = self.uid_queue.get()
            try:
                self.travel(uid, self.start_page)
            except:
            #except (TimeoutException, WebDriverException) as e:
                time.sleep(5)
                self.travel(uid, self.start_page)
            time.sleep(5)
        self.client.quit()
        
    def travel(self, uid, start_page):
        if not uid:
            return None
        info_url = 'http://weibo.cn/u/' + uid
        print 'open the %s user %s info page' % (self.total_uids.index(uid), uid)
        self.client.get(info_url)
        home_page_soup = BeautifulSoup(self.client.page_source)
        tip_div = home_page_soup.find('div', {'class': 'tip2'})
        if tip_div:
            statuses_count = int(tip_div.find('span', {'class': 'tc'}).string[3:-1])
            a_eles = tip_div.findAll('a')
            friends_count = int(a_eles[0].string[3:-1])
            followers_count = int(a_eles[1].string[3:-1])
            print statuses_count, friends_count, followers_count
            db.users.save({'_id': int(uid), 'statuses_count': statuses_count, 'friends_count': friends_count, 'followers_count': followers_count})
            time.sleep(5)
            

def main():
    start_idx = int(sys.argv[1])
    end_idx = int(sys.argv[2])
    try:
        start_page = int(sys.argv[3])
    except:
        start_page = 1
    print 'spider range %s -- %s, start from page %s ' % (start_idx, end_idx, start_page)

    total_uids = []
    uid_queue = Queue.Queue()

    count_idx = 0
    for line in open(r'./test/uidlist_20130918_missed_followers.txt').readlines():
        if line.startswith(codecs.BOM_UTF8):
            line = line[3:]
        uid = line.strip().split(' ')[0]
        if count_idx >= start_idx and count_idx <= end_idx:
            uid_queue.put(uid)
        total_uids.append(uid)
        count_idx += 1

    WEIBO_USER = raw_input('please input you account name: ')
    WEIBO_PWD = getpass('please input you account password: ')
    s = Spider(WEIBO_USER, WEIBO_PWD)
    s.spider(start_page, uid_queue, total_uids, True)

if __name__ == '__main__':
    main()