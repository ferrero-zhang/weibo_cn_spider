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

GSID = 'gsid_CTandWM=4uCBa7521QnK50LatVtiKcsTM3l' #naive way
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

start_idx = 3#int(sys.argv[1])
end_idx = 3#int(sys.argv[2])

try:
    start_page = int(sys.argv[3])
except:
    start_page = 1

print 'spider range %s -- %s, start from page %s ' % (start_idx, end_idx, start_page)

uid_queue = Queue.Queue()  ##用户队列,先进先出
##从文件中读入ID
f = open(r'uidlist_20130828.txt')

s = []

count_idx = 0
for line in f.readlines():
    if line.startswith(codecs.BOM_UTF8):
        line = line[3:]
    uid = line.strip().split(' ')[0]
    if count_idx >= start_idx and count_idx <= end_idx:
        s.append(uid)
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
        
    def travel(self, uid=None, startstr='20120209', endstr='20120309'):
        if not uid:
            return None
        #url = 'http://weibo.cn/u/'+uid
        weibo_profile_url = 'http://weibo.cn/' + uid + '/profile?keyword=%E5%BE%AE%E5%8D%9A&hasori=0&haspic=0&starttime=' + startstr + '&endtime=' + endstr + '&advancedfilter=1&st=8786'
        #经测试，只能抓100页数据，即1000条数据，需要记录total_page > 100的uid
        total_page = 1
        #home_page_soup = BeautifulSoup(self.client.urlopen(url+'?page=1'))
        print 'open %s weibo profile page 1 ' % uid
        home_page_soup = BeautifulSoup(self.client.urlopen(weibo_profile_url +'&page=1'))
        try:
            total_page = int(home_page_soup.find('div', {'class':'pa', 'id':'pagelist'}).form.div.\
                             find('input', {'name':'mp'})['value'])
            print 'total_page: ', total_page
            if total_page > 100:
                total_page = 100
        except Exception, e:
            #no status or status = 1 page
            pass
        try:
            print 'open %s user profile page' % uid
            name, verified, gender, location, desc, tags = self._travel_info(uid)
            print 'spider %d searching uid: %s name: %s...' % (self.num, uid, name)
        except Exception, e:
            print 'User %s Info Page Error:%s' % (uid, e)
            return None
        if not name or not gender or not location:
            #user information missed
            print 'User %s Info Page Missed' % uid
            return None
        weibo_user_url_pattern = r'http://weibo.cn/u/(\d+)\D*'

        posts = []

        for current_page in range(start_page, total_page+1):
            if current_page > 1:
                #home_page_soup = BeautifulSoup(self.client.urlopen(url+'?page='+str(current_page)), parseOnlyThese=SoupStrainer('div', {'class': 'c'}))
                print 'open %s weibo profile page %s ' % (uid, current_page)
                home_page_soup = BeautifulSoup(self.client.urlopen(weibo_profile_url + '&page='+str(current_page)), parseOnlyThese=SoupStrainer('div', {'class': 'c'}))
 
            #print home_page_soup.findAll('div', {'class': 'c'})
            for status in home_page_soup.findAll('div', {'class': 'c'})[:-2]:
                #print status
                try:
                    mid = status['id'][2:]
                    print mid
                except Exception, e:
                    #no status publish
                    print 'here10'
                    continue
                status_divs = status.findAll('div')
                #print status_divs
                status_divs_count = len(status_divs)
                if status_divs_count == 3:
                    # text & picture & repost_text
                    div = status_divs[0]
                    cmt = div.find('span', {'class': 'cmt'})
                    try:
                        #some weibo may be deleted
                        source_user_a_tag = cmt.contents[1]
                        source_user_url = source_user_a_tag['href']
                        source_user_name = source_user_a_tag.string
                    except:
                        print 'here10'
                        continue
                    ctt = div.find('span', {'class': 'ctt'})
                    source_text = ''
                    for ctt_tag in ctt.contents:
                        try:
                            source_text += clean_html(ctt_tag.string)
                        except:
                            pass
                    source_text.strip()
                    if not source_text:
                        print 'here9'
                        continue
                    #print source_user_name+':'+source_text  ##打印出来啦,转发的原文本
                    re_div = status_divs[2]
                    re_text = ''
                    for re_tag in re_div.contents[1:-9]:
                        try:
                            re_text += clean_html(re_tag.string)
                        except:
                            pass
                    re_text.strip()
                    if not re_text:
                        print 'here8'
                        continue
                    ct_span = re_div.find('span', {'class': 'ct'})
                    if len(ct_span.contents)> 1:
                        creat_at = ct_span.contents[0][:-8] #remove &nbsp;来自
                        ts = int(smc2unix(creat_at))
                        source = ct_span.contents[1].string
                    else:
                        tokens = ct_span.string.split('&nbsp;')
                        ts = int(smc2unix(tokens[0].strip()))
                        source = tokens[1][2:].strip()
                    #print source, unix2localtime(ts)  ##打印出来啦
                    content = re_text+' '+source_text
                    content, urls, hashtags, emotions = clean_status(content)

                    r_source_user_uid = re.search(weibo_user_url_pattern, source_user_url)
                    if r_source_user_uid:
                        if r_source_user_uid.group(1) == None:
                            print 'open source user profile page to get uid '
                            source_user_uid = self._getuid(source_user_url)
                        else:
                            source_user_uid = r_source_user_uid.group(1)
                    else:
                        print 'open source user profile page to get uid '
                        source_user_uid = self._getuid(source_user_url)  ##TimeoutError:HTTPConnectionPool(host='weibo.cn', port=None): Request timed out.
                    print 'open source user %s user profile page' % source_user_uid
                    source_user_name, source_user_verified, source_user_gender, source_user_location, \
                        source_user_desc, source_user_tags = self._travel_info(source_user_uid)
                    if not source_user_name or not source_user_gender or not source_user_location:
                        print 'Repost User %s Missed' % source_user_uid
                        print 'here7'
                        continue
                    post = {'_id': mid,
                            'uid': uid,
                            'name': name,
                            'gender': gender,
                            'location': location,
                            'text': re_text,
                            'repost': {'uid': source_user_uid,
                                       'name': source_user_name,
                                       'gender': source_user_gender,
                                       'location': source_user_location,
                                       'text': source_text},
                            'source': source,
                            'ts': ts,
                            'urls': urls,
                            'hashtags': hashtags,
                            'emotions': emotions}
                elif status_divs_count == 2:
                    div = status_divs[0]

                    ctt = div.find('span', {'class': 'ctt'})
                    source_text = ''
                    for ctt_tag in ctt.contents:
                        try:
                            source_text += clean_html(ctt_tag.string)
                        except:
                            pass
                    source_text.strip()
                    if not source_text:
                        print 'here6'
                        continue
                    
                    cmt = div.find('span', {'class': 'cmt'})
                    if not cmt:
                        #text & picture
                        pic_div = status_divs[1]
                        ct_span = pic_div.find('span', {'class': 'ct'})
                        if len(ct_span.contents)> 1:
                            creat_at = ct_span.contents[0][:-8] #remove &nbsp;来自
                            ts = int(smc2unix(creat_at))
                            source = ct_span.contents[1].string
                        else:
                            tokens = ct_span.string.split('&nbsp;')
                            ts = int(smc2unix(tokens[0].strip()))
                            source = tokens[1][2:].strip()
                        #print source, unix2localtime(ts)  ##打印出来啦
                        content = source_text
                        content, urls, hashtags, emotions = clean_status(content)
                        post = {'_id': mid,
                                'uid': uid,
                                'name': name,
                                'gender': gender,
                                'location': location,
                                'text': source_text,
                                'source': source,
                                'ts': ts,
                                'urls': urls,
                                'hashtags': hashtags,
                                'emotions': emotions}
                    else:
                        #text & repost text
                        try:
                            #some weibo may be deleted
                            source_user_a_tag = cmt.contents[1]
                            source_user_url = source_user_a_tag['href']
                            source_user_name = source_user_a_tag.string
                        except:
                            print 'source weibo has been deleted'
                            continue

                        re_div = status_divs[1]
                        re_text = ''
                        for re_tag in re_div.contents[1:-9]:
                            try:
                                re_text += clean_html(re_tag.string)
                            except:
                                pass
                        re_text.strip()
                        if not re_text:
                            print 'here4'
                            continue
                        ct_span = re_div.find('span', {'class': 'ct'})
                        if len(ct_span.contents)> 1:
                            creat_at = ct_span.contents[0][:-8] #remove &nbsp;来自
                            ts = int(smc2unix(creat_at))
                            source = ct_span.contents[1].string
                        else:
                            tokens = ct_span.string.split('&nbsp;')
                            ts = int(smc2unix(tokens[0].strip()))
                            source = tokens[1][2:].strip()
                        #print source, unix2localtime(ts)  ##打印出来啦
                        content = re_text+' '+source_text
                        content, urls, hashtags, emotions = clean_status(content)

                        r_source_user_uid = re.search(weibo_user_url_pattern, source_user_url)
                        if r_source_user_uid:
                            if r_source_user_uid.group(1) == None:
                                print 'open source user profile page to get uid '
                                source_user_uid = self._getuid(source_user_url)
                            else:
                                source_user_uid = r_source_user_uid.group(1)
                        else:
                            print 'open source user profile page to get uid '
                            source_user_uid = self._getuid(source_user_url)  ##TimeoutError:HTTPConnectionPool(host='weibo.cn', port=None): Request timed out.

                        print 'open source user %s user profile page' % source_user_uid   
                        source_user_name, source_user_verified, source_user_gender, source_user_location, \
                            source_user_desc, source_user_tags = self._travel_info(source_user_uid)
                        if not source_user_name or not source_user_gender or not source_user_location:
                            print 'Repost User %s Missed' % source_user_uid
                            print 'here3'
                            continue
                        post = {'_id': mid,
                                'uid': uid,
                                'name': name,
                                'gender': gender,
                                'location': location,
                                'text': re_text,
                                'repost': {'uid': source_user_uid,
                                           'name': source_user_name,
                                           'gender': source_user_gender,
                                           'location': source_user_location,
                                           'text': source_text},
                                'source': source,
                                'ts': ts,
                                'urls': urls,
                                'hashtags': hashtags,
                                'emotions': emotions}
                elif status_divs_count == 1:
                    #text
                    div = status_divs[0]
                    ctt = div.find('span', {'class': 'ctt'})
                    source_text = ''
                    for ctt_tag in ctt.contents:
                        try:
                            source_text += clean_html(ctt_tag.string)
                        except exception, e:
                            pass
                    source_text.strip()
                    if not source_text:
                        print 'here2'
                        continue
                    #print source_text
                    ct_span = div.find('span', {'class': 'ct'})
                    if len(ct_span.contents)> 1:
                        creat_at = ct_span.contents[0][:-8] #remove &nbsp;来自
                        ts = int(smc2unix(creat_at))
                        source = ct_span.contents[1].string
                    else:
                        tokens = ct_span.string.split('&nbsp;')
                        ts = int(smc2unix(tokens[0].strip()))
                        source = tokens[1][2:].strip()
                    #print source, unix2localtime(ts)  ##打印出来啦
                    content = source_text
                    content, urls, hashtags, emotions = clean_status(content)
                    post = {'_id': mid,
                            'uid': uid,
                            'name': name,
                            'gender': gender,
                            'location': location,
                            'text': source_text,
                            'source': source,
                            'ts': ts,
                            'urls': urls,
                            'hashtags': hashtags,
                            'emotions': emotions}
                else:
                    print 'here1'
                    continue
                #print post
                db.users.save(post) ##存入数据库
                posts.append(post)
            time.sleep(5)

        #print post['_id'] + '  ' + post['uid'] + '  ' + post['name'] + '  ' + post['gender'] + '  ' + post['location']
        return posts

    def _travel_info(self, uid):
        
        url = 'http://weibo.cn/'+uid+'/info'   ##TypeError:cannot concatenate 'str' and 'NoneType' objects(source_user_uid)
        
        name, verified, gender, location, desc, tags = None, None, None, None, None, None
        home_page_soup = BeautifulSoup(self.client.urlopen(url))
        user_info_div = home_page_soup.findAll('div', {'class': 'c'})

        try:
            user_info_div  = user_info_div[2]   ##IndexError:list index out of range
        except IndexError, e:
            print e
        for br_tag in user_info_div.contents:
            info_string = br_tag.string
            if info_string:
                rname = re.search(u'昵称:(.+)', info_string)
                if rname:
                    name = rname.group(1)
                    continue
                rverified = re.search(u'认证:(.+)', info_string)
                if rverified:
                    verified = rverified.group(1)
                    continue  
                rgender = re.search(u'性别:(.+)', info_string)
                if rgender:
                    gender = rgender.group(1)
                    continue
                rlocation = re.search(u'地区:(.+)', info_string)
                if rlocation:
                    location = rlocation.group(1)
                    continue
                rdesc = re.search(u'简介:(.+)', info_string)
                if rdesc:
                    desc = rdesc.group(1)
                    continue
        a_tags = user_info_div.findAll('a')[:-1]
        if a_tags:
            tags = []
            for a_tag in a_tags:
                try:
                    b = a_tag.contents[0]
                    tags.append(b)
                except:
                    pass
        return name, verified, gender, location, desc, tags

    def _getuid(self, url):
        home_page_soup = BeautifulSoup(self.client.urlopen(url))
        uid = None
        user_info_div = home_page_soup.find('div', {'class': 'ut'})
        for user_info_div_a in user_info_div.findAll('a'):
            user_info_div_a_result = re.search(r'/(\d+)/info', user_info_div_a['href'])
            if  user_info_div_a_result:
                uid =  user_info_div_a_result.group(1)
                break
        return uid

def main():
    s = Spider()
    s.spider(join=True)

if __name__ == '__main__':
    #pass
    main()

