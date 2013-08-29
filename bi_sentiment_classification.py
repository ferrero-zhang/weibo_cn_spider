# -*- coding: utf-8 -*-
from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import load_scws
from xapian_weibo.utils import cut
from gensim import corpora, models, similarities

cut_str = load_scws()

HAPPY = 1
ANGRY = 2

def emoticon(pe_set, ne_set, text):
    """ text是微博文本，不是关键词"""

    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)
    ps = 0
    ns = 0

    if remotions:
        for e in remotions:
            if e in pe_set:
                ps = 1
            elif e in ne_set:
                ns = 1

    state = 0
    if ps == 1 and ns == 0:
        state = HAPPY
    elif ps == 0 and ns == 1:
        state = ANGRY

    return state

def get_p_n_set():
	##从txt文件中获取选定积极、消极表情符号的集合
	pe_set = set([])
	ne_set = set([])
	seed_set = set([])
	with open('/home/mirage/sentiment/new_seed_emoticons.txt') as f:
	    for l in f:
	        pair = l.rstrip().split(':')
	        seed_set.add(pair[0].decode('utf-8'))
	        try:
	            if pair[1]=='1':
	                pe_set.add(pair[0])
	            else:
	                ne_set.add(pair[0])
	        except:
	            print pair
	print 'p_set,n_set:',len(pe_set),len(ne_set)
	return pe_set, ne_set

def get_dictionary():
	##读取字典
	##读取各个词的权重信息
	loop = 8
	dictionary_1 =corpora.Dictionary.load('/home/mirage/sentiment/subjective_54W_'+str(loop)+'620.dict')
	step1_score = {}
	with open('/home/mirage/sentiment/new_emoticon_54W_'+str(loop)+'620.txt') as f:
	    for l in f:
	        lis = l.rstrip().split()
	        step1_score[int(lis[0])] = [float(lis[1]),float(lis[2])]

	print dictionary_1
	print len(step1_score)
	for i in range(5):
	    print step1_score[i]

	dictionary_2 =corpora.Dictionary.load('/home/mirage/sentiment/polarity_270W_'+str(loop)+'620.dict')
	step2_score = {}
	with open('/home/mirage/sentiment/polarity_270W_'+str(loop)+'620.txt') as f:
	    for l in f:
	        lis = l.rstrip().split()
	        step2_score[int(lis[0])] = [float(lis[1]),float(lis[2])]

	print dictionary_2
	print len(step2_score)
	for i in range(5):
	    print step2_score[i]
	return dictionary_1, dictionary_2, step1_score, step2_score

def bi_classification(mid_text):
	dictionary_1, dictionary_2, step1_score, step2_score = get_dictionary()
	triple = [0, 0, 0]
	iter_count = 0
	ts = te = time.time()
	f_senti = open('bi_sentiment.txt', 'w')
	for mid, text in mid_text.iteritems():
	    if iter_count % 10000 == 0:
	        te = time.time()
	        print iter_count, '%s sec' % (te - ts)
	        ts = te
	    iter_count += 1
	    sentiment = 0
	    mid_id_str = id_str = str(mid)

	    if text != '':
	        entry = cut(cut_str, text)

	        bow = dictionary_1.doc2bow(entry)
	        sub_score = [1,1]

	        for pair in bow:
	            sub_score[0] *= (step1_score[pair[0]][0]**pair[1])
	            sub_score[1] *= (step1_score[pair[0]][1]**pair[1])
	        if sub_score[0]<sub_score[1]:
	            s_bow = dictionary_2.doc2bow(entry)
	            score2 = [1,1]
	            for pair in s_bow:
	                score2[0] *= (step2_score[pair[0]][0]**pair[1])
	                score2[1] *= (step2_score[pair[0]][1]**pair[1])
	            if score2[0] > score2[1]:
	                sentiment = HAPPY
	            elif score2[1] > score2[0]:
	                sentiment = ANGRY

	    f_senti.write('%s %s\n' % (id_str, sentiment))
	f_senti.close()
	    
if __name__ == '__main__':
	mid_text = {}
	bi_classification(mid_text)
