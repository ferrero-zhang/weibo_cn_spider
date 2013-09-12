# -*- coding: utf-8 -*-

import os
import re
import time
import urllib
import urllib2
import cookielib
import threading
import urllib3
import pymongo
import codecs
import Queue
import sys

from BeautifulSoup import BeautifulSoup, SoupStrainer

WEIBO_USER = 'xxx'
WEIBO_PWD = 'xxx'

GSID = 'gsid_CTandWM=4u5La7521lfDc0Xr4Ysiqbnfebt' #naive way
COOKIES_FILE = 'cookies.txt'

def con_database():##使用的时候导入，每个方法里有一个就行,注意不能更改
    DB_HOST = '219.224.135.60'
    DB_PORT = 27017
    DB_USER = 'root'
    DB_PWD = 'root'
    connection = pymongo.Connection(DB_HOST, DB_PORT)
    db = connection.admin
    db.authenticate(DB_USER, DB_PWD)
    return connection.test_crawler_liwenwen

db = con_database()

start_idx = int(sys.argv[1])
end_idx = int(sys.argv[2])

try:
    start_page = int(sys.argv[3])
except:
    start_page = 1

print 'spider range %s -- %s, start from page %s ' % (start_idx, end_idx, start_page)

total_uids = []
uid_queue = Queue.Queue()  ##用户队列,先进先出
##从文件中读入ID
f = open('./test/20130911_lhf_uid.txt')

s = []

count_idx = 0
for line in f.readlines():
    if line.startswith(codecs.BOM_UTF8):
        line = line[3:]
    uid = line.strip().split(' ')[0]
    if count_idx >= start_idx and count_idx <= end_idx:
        s.append(uid)
    total_uids.append(uid)
    if uid == None:
        print 'uid equals None'
    count_idx += 1
f.close()

l = len(s)


for i in range(0,l):
    uid_queue.put(s[i])
#add uid to queue for spider ##以用户“何兵”为开始

def load_cookies():
    '''模拟浏览器登录微博, 获取cookies字符串
    '''
    cj = cookielib.MozillaCookieJar()
    if os.path.isfile(COOKIES_FILE):
        cookie_str = ''
        cj.load(COOKIES_FILE)
        cookie_list = []
        for cookie in cj:
            if cookie.domain == '.sina.cn':
                cookie_list.append(str(cookie).split(' ')[1])
            cookie_str = ';'.join(cookie_list)
        return cookie_str

    mobile = WEIBO_USER
    password = WEIBO_PWD
    user_agent = '''Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us)
                    AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4
                    Mobile/7B334b Safari/531.21.10'''
    header = {'User-Agent': user_agent} ##把自身模拟成Internet Explorer
    
    login_url = 'http://login.weibo.cn/login/?ns=1&revalid=2&backURL=http%3A%2F%2Fweibo.cn%2F&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt='
    res = urllib2.urlopen(urllib2.Request(login_url, headers=header))##发送请求的同时传header单，注意缩写，res = response, req = request
    login_html = res.read()
    res.close()
    login_soup = BeautifulSoup(login_html)
    login_form_action = login_soup.find('form')['action']
    vk = pwd = submit = backURL = backTitle = None
    for input_box in login_soup.findAll('input'):
        if input_box['type'] == 'password':
            pwd = input_box['name']
        elif input_box['type'] == 'submit':
            submit = input_box['value']
        elif input_box['type'] == 'hidden':
            if input_box['name'] == 'vk':
                vk = input_box['value']
            elif input_box['name'] == 'backURL':
                backURL = input_box['value']
            elif input_box['name'] == 'backTitle':
                backTitle = input_box['value']
    submit = '%E7%99%BB%E5%BD%95' #登录
    params = urllib.urlencode({'mobile': mobile, pwd: password, 'remember': 'on',
                               'backURL': backURL, 'vk': vk, 'submit': submit, 'tryCount': ''})
    print 'login post params %s' % params
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    submit_url = 'http://login.weibo.cn/login/' + login_form_action
    print 'submit url %s' % submit_url
    res = opener.open(urllib2.Request(submit_url, headers=header), params)
    redirect_html = res.read()
    res.close()
    redirect_soup = BeautifulSoup(redirect_html)
    redirect_url = redirect_soup.find('a')['href']
    res = opener.open(urllib2.Request(redirect_url, headers=header))
    res.close()
    cj.save(COOKIES_FILE, ignore_discard=True)
    cookie_list = []
    cookie_str = ''
    for cookie in cj:
        if cookie.domain == '.sina.cn':
            cookie_list.append(str(cookie).split(' ')[1])
        cookie_str = ';'.join(cookie_list)
    return cookie_str

def unix2localtime(ts):
    '''将unix时间戳转化为本地时间格式: 2011-1-1 11:11:11
    '''
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))

def smc2unix(date_str):
    '''将微博上的时间格式转化为unix时间戳
    '''
    time_pattern_1 = r'(\d+)'+u'分钟前' #5分钟前
    time_pattern_2 = u'今天'+r' (\d\d):(\d\d)' #今天 17:51
    time_pattern_3 = r'(\d\d)'+u'月'+r'(\d\d)'+u'日 '+r'(\d\d):(\d\d)' #02月22日 08:32 
    time_pattern_4 =  r'(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)' #2011-12-31 23:34:19
    date_str.strip()
    try:
        minute = int(re.search(time_pattern_1, date_str).group(1))
        now_ts = time.time()
        ts = now_ts - minute*60
        #print time.localtime(ts)
        return ts
    except:
        pass
    try:
        hour = re.search(time_pattern_2, date_str).group(1)
        minute = re.search(time_pattern_2, date_str).group(2)
        now = time.localtime()
        date = str(now.tm_year)+'-'+str(now.tm_mon)+'-'+str(now.tm_mday)+' '+hour+':'+minute
        ts = time.mktime(time.strptime(date, '%Y-%m-%d %H:%M'))
        #print time.localtime(ts)
        return ts
    except:
        pass
    try:
        month = re.search(time_pattern_3, date_str).group(1)
        day = re.search(time_pattern_3, date_str).group(2)
        hour = re.search(time_pattern_3, date_str).group(3)
        minute = re.search(time_pattern_3, date_str).group(4)
        now = time.localtime()
        date = str(now.tm_year)+'-'+month+'-'+day+' '+hour+':'+minute
        ts = time.mktime(time.strptime(date, '%Y-%m-%d %H:%M'))
        #print time.localtime(ts)
        return ts
    except:
        pass
    try:
        year = re.search(time_pattern_4, date_str).group(1)
        month = re.search(time_pattern_4, date_str).group(2)
        day = re.search(time_pattern_4, date_str).group(3)
        hour = re.search(time_pattern_4, date_str).group(4)
        minute = re.search(time_pattern_4, date_str).group(5)
        second = re.search(time_pattern_4, date_str).group(6)
        date = year+'-'+month+'-'+day+' '+hour+':'+minute+':'+second
        ts = time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S'))
        #print time.localtime(ts)
        return ts
    except:
        pass
    return None

def clean_status(status_text):
    '''清洗微博内容 返回清洗后的文本、文本中包含的#话题#、文本中的[表情]
    '''
    t_url_pattern = r'http://t.cn/\w+'  #文本
    tag_pattern = r'#(.+?)#'            #话题   #上半年微盘点# 
    emotion_pattern = r'\[(.+?)\]'      #表情   [酷]
    content = re.sub(t_url_pattern, ' ', status_text)
    content = re.sub(tag_pattern, r'\1', content)
    content = re.sub(emotion_pattern, r'', content)
    #print content
    urls = None
    mentions = None
    tags = None
    emotions = None
    rurls = re.findall(t_url_pattern, status_text)  ##findall列出字符串中模式的所有匹配项
    if rurls:
        urls = []
        for url in rurls:
            urls.append(url)
    rtags = re.findall(tag_pattern, status_text)
    if rtags:
        tags = []
        for tag in rtags:
            #print tag  ##8,12之类的东西 ？？？
            tags.append(tag)
    remotions = re.findall(emotion_pattern, status_text)
    if remotions:
        emotions = []
        for emotion in remotions:
            #print emotion
            emotions.append(emotion)
    if tags:
        tags = list(set(tags))
    content = clean_non_alphanumerics(content)
    return content, urls, tags, emotions

def clean_non_alphanumerics(content):
    '''清除文本中除了中文、字母、数字和空格外的其他字符
    '''
    char_list = []
    for char in content:
        n = ord(char)
        if 0x4e00<=n<0x9fa6 or 97<=n<=122 or 65<=n<=90 or 48<=n<=57:
            char_list.append(char)
    return ''.join(char_list)

def clean_html(html):
    '''清除html文本中的相关转义符号
    '''
    html = re.sub('&nbsp;', '', html)
    html = re.sub('&ensp;', '', html)
    html = re.sub('&emsp;', '', html)
    html = re.sub('&amp;', '&', html)
    html = re.sub('&lt;', '<', html)
    html = re.sub('&gt;', '>', html)
    html = re.sub('&quot;', '"', html)
    return html

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

    def spider(self, join=False):
        st = SpiderThread(1, controler=self, client=self.client)
        st.setDaemon(True)
        st.start()
        self.spiders.append(st)
        if join:
            st.join()

class SpiderThread(threading.Thread):  ##创建一个线程对象    getName()是threading.Thread类的一个方法，用来获得这个线程对象的name。还有一个方法setName()当然就是来设置这个线程对象的name的了。
    def __init__(self, num, controler=None, client=None):
        self.num = num
        self.controler = controler
        self.client = client
        threading.Thread.__init__(self)
    
    def run(self):   ##重写run方法
        while not uid_queue.empty():
            uid = uid_queue.get()
            self.travel(uid=uid)
            time.sleep(5)
        
    def travel(self, uid=None):
        if not uid:
            return None
        fans_url = 'http://weibo.cn/' + uid + '/fans?vt=4&st=78eb'
        total_page = 1
        print 'open the %s user %s fans page 1 ' % (total_uids.index(uid), uid)
        home_page_soup = BeautifulSoup(self.client.urlopen(fans_url +'&page=1'))
        try:
            total_page = int(home_page_soup.find('div', {'class':'pa', 'id':'pagelist'}).form.div.\
                             find('input', {'name':'mp'})['value'])
            print 'total_page: ', total_page
        except Exception, e:
            #no friends or followers or = 1 page
            print 'total_page: ', 1
        followers_list = []
        for current_page in range(start_page, total_page+1):
            if current_page > 1:
                print 'open the %s user %s fans page %s ' % (total_uids.index(uid), uid, current_page)
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
    s = Spider()
    s.spider(join=True)

if __name__ == '__main__':
    #pass
    main()
