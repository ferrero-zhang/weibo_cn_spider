# -*- coding: utf-8 -*-


import re
import time
import threading
import codecs
import Queue
import sys
from utils4spider import unix2localtime, smc2unix, clean_status, clean_html, base62_decode
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from config import WEIBO_USER, WEIBO_PWD, getDB
from selenium.common.exceptions import TimeoutException, WebDriverException
from getpass import getpass

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

    def spider(self, start_page, uid_queue, total_uids, join=False, end_page=10):
        st = SpiderThread(1, start_page, end_page, uid_queue, total_uids, controler=self, client=self.client)
        st.setDaemon(True)
        st.start()
        self.spiders.append(st)
        if join:
            st.join()

class SpiderThread(threading.Thread):  ##创建一个线程对象 getName()是threading.Thread类的一个方法，用来获得这个线程对象的name。还有一个方法setName()当然就是来设置这个线程对象的name的了。
    def __init__(self, num, start_page, end_page, uid_queue, total_uids, controler=None, client=None):
        self.num = num
        self.controler = controler
        self.client = client
        self.start_page = start_page
        self.end_page = end_page
        self.uid_queue = uid_queue
        self.total_uids = total_uids
        self.startstr = '20090101'
        self.endstr = '20130916'
        threading.Thread.__init__(self)
    
    def run(self):   ##重写run方法
        while not self.uid_queue.empty():
            uid = self.uid_queue.get()
            try:
                self.travel(uid, self.startstr, self.endstr, self.start_page, self.end_page)
            except:
            #except (TimeoutException, WebDriverException) as e:
                time.sleep(5)
                self.travel(uid, self.startstr, self.endstr, self.start_page, self.end_page)
            time.sleep(5)
        self.client.quit()
        
    def travel(self, uid, startstr, endstr, start_page, end_page):
        if not uid:
            return None
        weibo_profile_url = 'http://weibo.cn/' + uid + '/profile?keyword=%E5%BE%AE%E5%8D%9A&hasori=0&haspic=0&starttime=' + startstr + '&endtime=' + endstr + '&advancedfilter=1&st=8786'
        #经测试，只能抓100页数据，即1000条数据，需要记录total_page > 100的uid
        total_page = 1
        print 'open the %s user %s weibo profile page 1 ' % (self.total_uids.index(uid),uid)
        self.client.get(weibo_profile_url +'&page=1')
        home_page_soup = BeautifulSoup(self.client.page_source)
        try:
            total_page = int(home_page_soup.find('div', {'class':'pa', 'id':'pagelist'}).form.div.\
                             find('input', {'name':'mp'})['value'])
            print 'total_page: ', total_page
            if total_page > 100:
                total_page = 100
        except Exception, e:
            #no status or status = 1 page
            print 'total_page: ', 1
        '''
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
        '''
        if end_page and end_page < total_page:
            end_page = end_page
        else:
            end_page = total_page
        for current_page in range(start_page, end_page+1):
            if current_page > 1:
                print 'open the %s user %s weibo profile page %s ' % (self.total_uids.index(uid),uid, current_page)
                self.client.get(weibo_profile_url + '&page='+str(current_page))
                home_page_soup = BeautifulSoup(self.client.page_source, parseOnlyThese=SoupStrainer('div', {'class': 'c'}))

            if home_page_soup.findAll('div', {'class': 'c'})[:-2] == []:
                print 'page ',current_page, ' has no content'
            else:
                for status_soup in home_page_soup.findAll('div', {'class': 'c'})[:-2]:
                    post = self._soup2WeiboItem(status_soup, uid)
                    if post != None:
                        db.weibos.save(post) ##存入数据库
            time.sleep(5)

    def _soup2WeiboItem(self, status, uid):
        weibo_user_url_pattern = r'http://weibo.cn/u/(\d+)\D*'
        try:
            #mid = base62_decode(status['id'][2:])
            mid = status['id'][2:]
            print mid
        except Exception, e:
            #no status publish
            print 'no status publish'
            return None
        status_divs = status.findAll('div')
        #print status_divs
        status_divs_count = len(status_divs)
        if status_divs_count == 3:
            # text & picture & repost_text
            div = status_divs[0]
            cmt = div.find('span', {'class': 'cmt'})
            kt = div.find('span', {'class': 'kt'})
            if kt and kt.contents[0][1:13].encode('utf-8') != '此内容为不实消息，已处理':
                #置顶微博
                print 'top weibo'
                cmt = div.findAll('span', {'class': 'cmt'})[1]
            try:
                #some weibo may be deleted
                source_user_a_tag = cmt.contents[1]
                source_user_url = source_user_a_tag['href']
                source_user_name = source_user_a_tag.string
            except:
                print 'source weibo has been deleted'
                return None
            
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
                return None

            sec_div = status_divs[1]
            retweeted_attitudes_count = int(sec_div.findAll('span', {'class': 'cmt'})[0].string[2:-1])
            retweeted_reposts_count = int(sec_div.findAll('span', {'class': 'cmt'})[1].string[5:-1])
            retweeted_comments_count = int(sec_div.find('a', {'class': 'cc'}).string[5:-1])
            print retweeted_attitudes_count, retweeted_reposts_count, retweeted_comments_count

            re_div = status_divs[2]
            counts = re_div.findAll('a')[-5:]
            try:#来源平台是span
                comments_count = int(counts[-2].string[3:-1])
                try:#未赞
                    attitudes_count = int(counts[-4].string[2:-1])
                except:#已赞
                    attitudes_count = int(re_div.findAll('span', {'class': 'cmt'})[-1].string[3:-1])
                reposts_count = int(counts[-3].string[3:-1])
            except:#来源平台是链接
                comments_count = int(counts[-3].string[3:-1])
                try:#未赞
                    attitudes_count = int(counts[-5].string[2:-1])
                except:#已赞
                    attitudes_count = int(re_div.findAll('span', {'class': 'cmt'})[-1].string[3:-1])
                reposts_count = int(counts[-4].string[3:-1])
            print attitudes_count, reposts_count, comments_count

            re_text = ''
            for re_tag in re_div.contents[1:-9]:
                try:
                    re_text += clean_html(re_tag.string)
                except:
                    pass
            re_text.strip()
            if not re_text:
                print 'here8'
                return None
            ct_span = re_div.find('span', {'class': 'ct'})
            if len(ct_span.contents)> 1:
                creat_at = ct_span.contents[0][:-8] #remove &nbsp;来自
                ts = int(smc2unix(creat_at))
                source = ct_span.contents[1].string
            else:
                tokens = ct_span.string.split('&nbsp;')
                ts = int(smc2unix(tokens[0].strip()))
                source = tokens[1][2:].strip()

            content = re_text+' '+source_text
            content, urls, hashtags, emotions = clean_status(content)
            '''
            r_source_user_uid = re.search(weibo_user_url_pattern, source_user_url)
            
            if r_source_user_uid:
                if r_source_user_uid.group(1) == None:
                    print 'open source user profile page to get uid '
                    source_user_uid = self._getuid(source_user_url)
                else:
                    source_user_uid = r_source_user_uid.group(1)
            else:
                print 'open source user profile page to get uid '
                source_user_uid = self._getuid(source_user_url)
            
            print 'open source user %s user profile page' % source_user_uid
            source_user_name, source_user_verified, source_user_gender, source_user_location, \
                source_user_desc, source_user_tags = self._travel_info(source_user_uid)
            if not source_user_name or not source_user_gender or not source_user_location:
                print 'Repost User %s Missed' % source_user_uid
                print 'here7'
                return None
            '''
            post = {'_id': mid,
                    'uid': uid,
                    'text': re_text,
                    'repost': {'text': source_text,
                               'retweeted_attitudes_count': retweeted_attitudes_count,
                               'retweeted_reposts_count': retweeted_reposts_count,
                               'retweeted_comments_count': retweeted_comments_count},
                    'source': source,
                    'ts': ts,
                    'urls': urls,
                    'hashtags': hashtags,
                    'emotions': emotions,
                    'attitudes_count': attitudes_count,
                    'reposts_count': reposts_count,
                    'comments_count': comments_count}
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
                return None

            kt = div.find('span', {'class': 'kt'})
            if kt:
                #置顶微博
                print 'top weibo'
                try:
                    cmt = div.findAll('span', {'class': 'cmt'})[1]
                except:
                    cmt = None
            else:
                cmt = div.find('span', {'class': 'cmt'})                    
            if not cmt:
                #text & picture
                pic_div = status_divs[1]
                a_counts = pic_div.findAll('a')
                attitudes_count = int(a_counts[2].string[2:-1])
                reposts_count = int(a_counts[3].string[3:-1])
                comments_count = int(a_counts[4].string[3:-1])
                print attitudes_count, reposts_count, comments_count
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
                        'text': source_text,
                        'source': source,
                        'ts': ts,
                        'urls': urls,
                        'hashtags': hashtags,
                        'emotions': emotions,
                        'attitudes_count': attitudes_count,
                        'reposts_count': reposts_count,
                        'comments_count': comments_count}
            else:
                #text & repost text
                try:
                    #some weibo may be deleted
                    source_user_a_tag = cmt.contents[1]
                    source_user_url = source_user_a_tag['href']
                    source_user_name = source_user_a_tag.string
                except:
                    print 'source weibo has been deleted'
                    return None

                sec_div = status_divs[0]
                retweeted_attitudes_count = int(sec_div.findAll('span', {'class': 'cmt'})[-2].string[2:-1])
                retweeted_reposts_count = int(sec_div.findAll('span', {'class': 'cmt'})[-1].string[5:-1])
                retweeted_comments_count = int(sec_div.find('a', {'class': 'cc'}).string[5:-1])
                print retweeted_attitudes_count, retweeted_reposts_count, retweeted_comments_count

                re_div = status_divs[1]
                counts = re_div.findAll('a')[-5:]
                try:#来源平台是span
                    comments_count = int(counts[-2].string[3:-1])
                    try:#未赞
                        attitudes_count = int(counts[-4].string[2:-1])
                    except:#已赞
                        attitudes_count = int(re_div.findAll('span', {'class': 'cmt'})[-1].string[3:-1])
                    reposts_count = int(counts[-3].string[3:-1])
                except:#来源平台是链接
                    comments_count = int(counts[-3].string[3:-1])
                    try:#未赞
                        attitudes_count = int(counts[-5].string[2:-1])
                    except:#已赞
                        attitudes_count = int(re_div.findAll('span', {'class': 'cmt'})[-1].string[3:-1])
                    reposts_count = int(counts[-4].string[3:-1])
                print attitudes_count, reposts_count, comments_count

                re_text = ''
                for re_tag in re_div.contents[1:-9]:
                    try:
                        re_text += clean_html(re_tag.string)
                    except:
                        pass
                re_text.strip()
                if not re_text:
                    print 'here4'
                    return None
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
                '''
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
                    return None
                '''
                post = {'_id': mid,
                        'uid': uid,
                        'text': re_text,
                        'repost': {'text': source_text,
                                   'retweeted_attitudes_count': retweeted_attitudes_count,
                                   'retweeted_reposts_count': retweeted_reposts_count,
                                   'retweeted_comments_count': retweeted_comments_count},
                        'source': source,
                        'ts': ts,
                        'urls': urls,
                        'hashtags': hashtags,
                        'emotions': emotions,
                        'attitudes_count': attitudes_count,
                        'reposts_count': reposts_count,
                        'comments_count': comments_count}
        elif status_divs_count == 1:
            #text
            div = status_divs[0]
            counts = div.findAll('a')[-5:]
            try:#来源平台是span
                comments_count = int(counts[-2].string[3:-1])
                try:#未赞
                    attitudes_count = int(counts[-4].string[2:-1])
                except:#已赞
                    attitudes_count = int(div.find('span', {'class': 'cmt'}).string[3:-1])
                reposts_count = int(counts[-3].string[3:-1])
            except:#来源平台是链接
                comments_count = int(counts[-3].string[3:-1])
                try:#未赞
                    attitudes_count = int(counts[-5].string[2:-1])
                except:#已赞
                    attitudes_count = int(div.find('span', {'class': 'cmt'}).string[3:-1])
                reposts_count = int(counts[-4].string[3:-1])
            print attitudes_count, reposts_count, comments_count
            ctt = div.find('span', {'class': 'ctt'})
            source_text = ''
            for ctt_tag in ctt.contents:
                try:
                    source_text += clean_html(ctt_tag.string)
                except Exception, e:
                    pass
            source_text.strip()
            if not source_text:
                print 'here2'
                return None
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
                    'text': source_text,
                    'source': source,
                    'ts': ts,
                    'urls': urls,
                    'hashtags': hashtags,
                    'emotions': emotions,
                    'attitudes_count': attitudes_count,
                    'reposts_count': reposts_count,
                    'comments_count': comments_count}
        else:
            print 'here1'
            return None
        return post
    '''
    def _travel_info(self, uid):
        
        url = 'http://weibo.cn/'+uid+'/info'   ##TypeError:cannot concatenate 'str' and 'NoneType' objects(source_user_uid)
        name, verified, gender, location, desc, tags = None, None, None, None, None, None
        self.client.get(url)
        home_page_soup = BeautifulSoup(self.client.page_source)
        user_info_div = home_page_soup.findAll('div', {'class': 'c'})
        try:
            user_info_div  = user_info_div[2]   ##IndexError:list index out of range
        except IndexError, e:
            print 'User %s doesnot exist' % uid
            return None
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
        self.client.get(url)
        home_page_soup = BeautifulSoup(self.client.page_source)
        uid = None
        user_info_div = home_page_soup.find('div', {'class': 'ut'})
        for user_info_div_a in user_info_div.findAll('a'):
            user_info_div_a_result = re.search(r'/(\d+)/info', user_info_div_a['href'])
            if  user_info_div_a_result:
                uid =  user_info_div_a_result.group(1)
                break
        return uid
    '''

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
    for line in open(r'./test/uidlist_20130918.txt').readlines():
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
