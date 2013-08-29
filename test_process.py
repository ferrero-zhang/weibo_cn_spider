# -*- coding: utf-8 -*-

import pymongo, time, codecs, datetime
try:
    from xapian_weibo.xapian_backend import XapianSearch
    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
except:
    pass

def con_database():
    DB_HOST = '219.224.135.60'
    DB_PORT = 27017
    DB_USER = 'root'
    DB_PWD = 'root'
    connection = pymongo.Connection(DB_HOST, DB_PORT)
    db = connection.admin
    db.authenticate(DB_USER, DB_PWD)
    return connection.test_crawler_liwenwen

def main(uid, startdate, enddate):
    startts = date2ts(startdate)
    endts = date2ts(enddate)
    db = con_database()
    print db.users.find({'uid': str(uid), 'ts':{'$gte': startts, '$lte': endts}}).count()
    cursor = db.users.find({'uid': str(uid), 'ts':{'$gte': startts, '$lte': endts}})
    for weibo in cursor:
        print weibo

def date2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def target_count(uidfile, startdate, enddate):
    mid_text = {}
    startts = date2ts(startdate)
    endts = date2ts(enddate)
    print startts, endts
    f = open(uidfile, 'r')
    db = con_database()
    total_count = 0
    for line in f.readlines():
        if line.startswith(codecs.BOM_UTF8):
            line = line[3:]
        uid = line.strip().split(',')[0]
        count = db.users.find({'uid': uid, 'ts':{'$gte': startts, '$lte': endts}}).count()
        cursor = db.users.find({'uid': uid, 'ts':{'$gte': startts, '$lte': endts}})
        for weibo in cursor:
            mid_text[weibo['_id']] = weibo['text']
            #print weibo['_id']
        '''
        if count != 0:
            print uid, count
        '''
        total_count += count
    print total_count
    f.close()
    return mid_text

def target_count_from_xapian(uidfile, startdate, enddate):
    mid_text = {}
    startts = date2ts(startdate)
    endts = date2ts(enddate)
    print 'startts: ', startts
    print 'endts:', endts
    f = open(uidfile, 'r')
    db = con_database()
    total_count = 0
    user_count = 0
    fw = open('uidlist_notin_xapian.txt', 'w')
    for line in f.readlines():
        if line.startswith(codecs.BOM_UTF8):
            line = line[3:]
        uid = int(line.strip().split(',')[0])
        query_dict = {'user': int(uid), 'timestamp':{'$gt': startts, '$lt': endts}}
        '''
        count = statuses_search.search(query=query_dict, count_only=True)
        '''
        count, get_results = statuses_search.search(query=query_dict, fields=['text', '_id'])
        for r in get_results():
            mid_text[mid_to_str(r['_id'])] = r['text']
            #print mid_to_str(r['_id'])
        '''
        if count != 0:
            #print uid, count
            user_count += 1
            #if user_count == 20:
            #    break
        else:
            fw.write('%s\n' % uid)
        total_count += count
        '''
    fw.close()
    f.close()
    #print total_count
    #print user_count
    return mid_text

def test():
    db = con_database()
    mid = 'y5cetjOgb'#'v99a9b9zv'#'y7Cli2BXw'#'y7Cnlm8EN'#'y8SDX61aE'#'y93ZKpIpP'
    print db.users.find({'_id': mid}).count()
    cursor = db.users.find({'_id': mid})
    for weibo in cursor:
        print weibo
        print weibo['text']
        print datetime.date.fromtimestamp(int(weibo['ts']))

def mid_to_str(mid):
    mid = str(mid)
    id1 = mid[0: 2]
    id2 = mid[2: 9]
    id3 = mid[9: 16]
    id_list = [id1, id2, id3]
    id_list = [base62_encode(int(mid)) for mid in id_list]
    return "".join(map(str, id_list))

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

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

def test_emotion(mid_text):
    from  bi_sentiment_classification import bi_classification
    bi_classification(mid_text)
    
if __name__ == '__main__':
    mid_text = target_count('uidlist_20130828.txt', '2012-02-09', '2012-03-10')
    mid_xapian_text = target_count_from_xapian('uidlist_20130828.txt', '2012-02-09', '2012-03-10')
    mid_text = dict(mid_text.items() + mid_xapian_text.items())
    test_emotion(mid_text) 
    #main(1681053824, '2012-02-09', '2012-03-10')
    #test()
