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
        if count != 0:
            print uid, count
        total_count += count
    print total_count
    f.close()

def target_count_from_xapian(uidfile, startdate, enddate):
    startts = date2ts(startdate)
    endts = date2ts(enddate)
    print 'startts: ', startts
    print 'endts:', endts
    f = open(uidfile, 'r')
    db = con_database()
    total_count = 0
    user_count = 0
    for line in f.readlines():
        if line.startswith(codecs.BOM_UTF8):
            line = line[3:]
        uid = int(line.strip().split(',')[0])
        query_dict = {'user': int(uid), 'timestamp':{'$gt': startts, '$lt': endts}}
        count = statuses_search.search(query=query_dict, count_only=True)
        '''
        count, get_results = statuses_search.search(query=query_dict, fields=['timestamp'])
        real_count = 0
        for r in get_results():
            if r['timestamp'] > startts and r['timestamp'] < endts:
                real_count += 1
        print count, real_count
        '''
        if count != 0:
            #print uid, count
            user_count += 1
        total_count += count
    print total_count
    print user_count
    f.close()

def test():
    db = con_database()
    mid = 'y5cetjOgb'#'v99a9b9zv'#'y7Cli2BXw'#'y7Cnlm8EN'#'y8SDX61aE'#'y93ZKpIpP'
    print db.users.find({'_id': mid}).count()
    cursor = db.users.find({'_id': mid})
    for weibo in cursor:
        print weibo
        print weibo['text']
        print datetime.date.fromtimestamp(int(weibo['ts']))
    
if __name__ == '__main__':
    #target_count('uidlist_20130828.txt', '2012-02-09', '2012-03-10')
    #target_count_from_xapian('uidlist_20130828.txt', '2012-02-09', '2012-03-10')
    #main(1681053824, '2012-02-09', '2012-03-10')
    test()
