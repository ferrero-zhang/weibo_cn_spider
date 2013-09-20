# -*- coding: utf-8 -*-


import re
import time
import threading
import urllib3
import codecs
import Queue
import sys
from utils4spider import load_cookies
from BeautifulSoup import BeautifulSoup, SoupStrainer
from config import WEIBO_USER, WEIBO_PWD, getDB, GSID

db = getDB()

class WeiboURL(object):
    def __init__(self):
        user_agent = '''Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us)
                        AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4
                        Mobile/7B334b Safari/531.21.10'''    #貌似是全球搜索引擎爬虫中的一种
 
        cookie_str = load_cookies()
        if not cookie_str:
            assert GSID, 'need GSID, a proof of successful login, for cookie'
            cookie_str = GSID

        self.headers = {'User-Agent': user_agent,
                        'Cookie': cookie_str}
        self.http_pool = urllib3.connection_from_url('weibo.cn', timeout=10, maxsize=3, headers=self.headers)

    def urlopen(self, url):
        print url
        res = self.http_pool.urlopen('GET', url, headers=self.headers)
        return res.data

class Spider(object):
    def __init__(self):
        self.spiders = []
        self.client = WeiboURL()

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
            self.travel(uid, self.start_page, self.end_page)
            time.sleep(5)
        
    def travel(self, uid, start_page, end_page):
        if not uid:
            return None
        fans_url = 'http://weibo.cn/' + uid + '/fans?vt=4&st=78eb'
        total_page = 1
        print 'open the %s user %s fans page 1 ' % (self.total_uids.index(uid), uid)
        home_page_soup = BeautifulSoup(self.client.urlopen(fans_url +'&page=1'))
        try:
            total_page = int(home_page_soup.find('div', {'class':'pa', 'id':'pagelist'}).form.div.\
                             find('input', {'name':'mp'})['value'])
            print 'total_page: ', total_page
        except Exception, e:
            #no friends or followers or = 1 page
            print 'total_page: ', 1
        if end_page and end_page < total_page:
            end_page = end_page
        else:
            end_page = total_page
        followers_list = []
        for current_page in range(start_page, end_page+1):
            if current_page > 1:
                print 'open the %s user %s fans page %s ' % (self.total_uids.index(uid), uid, current_page)
                home_page_soup = BeautifulSoup(self.client.urlopen(fans_url + '&page='+str(current_page)), parseOnlyThese=SoupStrainer('div', {'class': 'c'}))

            if home_page_soup.findAll('div', {'class': 'c'})[:-2] == []:
                print 'page ',current_page, ' has no content'
            else:
            	friend_soup = home_page_soup.findAll('div', {'class': 'c'})[:-2][0]
            	for friend_table in friend_soup.findAll('table'):
            	    friend_td = friend_table.findAll('td', {'valign': 'top'})[1]
                    if len(friend_td.findAll('a')) == 1:
                        friend_td = friend_table.findAll('td', {'valign': 'top'})[0]
                        friend_url = friend_td.find('img')['src']
                        friend_uid = re.match('.*cn/(\d*)/.*', friend_url).group(1)
                    else:
                	friend_url = friend_td.findAll('a')[1]['href']
            		friend_uid = re.match('.*uid=(\d*)&.*', friend_url).group(1)
            	    print friend_uid
            	    followers_list.append(friend_uid)
            time.sleep(5)
        db.followers.save({'_id': int(uid), 'followers': followers_list})
            

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

    s = Spider()
    s.spider(start_page, uid_queue, total_uids, True)

if __name__ == '__main__':
    main()