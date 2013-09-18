# -*- coding: utf-8 -*-
import time
import re


ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


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


def mid_to_str(mid):
    mid = str(mid)
    id1 = mid[0: 2]
    id2 = mid[2: 9]
    id3 = mid[9: 16]
    id_list = [id1, id2, id3]
    id_list = [base62_encode(int(mid)) for mid in id_list]
    return "".join(map(str, id_list))


def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def base62_decode(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number

    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num