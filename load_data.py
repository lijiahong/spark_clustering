# -*- coding:utf-8 -*-

import os
from utils import _default_mongo, getDataByName
from xapian_case.utils import load_scws, cut


s = load_scws()
cx_dict = set(['Ag','a','an','Ng','n','nr','ns','nt','nz','Vg','v','vd','vn','@','j'])
cx_dict_noun = set(['Ng','n','nr','ns','nt','nz'])

def load_data_from_mongo(topic, input_file):
    mongo = _default_mongo()
    results = getDataByName(mongo, topic)
    print "length:", len(results)

    inputs = []
    count = 0
    testFile = open(input_file,'w')
    for r in results:
        r['title'] = ''
        r['content168'] = r['content168'].encode('utf-8')
        r['content'] = r['content168']
        inputs.append(r)
        testFile.write(str(r['_id']))
        testFile.write('\t')
        testFile.write(r['content'])
        testFile.write('\n')
        count += 1
    print 'written', count
    testFile.close()
    return

def cut_words_local(input_file, output_file):
    tmp_file = open(input_file,"r")
    f = open(output_file,"w")
    count = 0
    for line in tmp_file.readlines():
        try:
            tid, text = line.split('\t')
        except:
            pass

        if not isinstance(text, str):
            raise ValueError("cut words input text must be string")
        cx_terms = cut(s, text, cx=True)
        terms = [term for term,cx in cx_terms if cx in cx_dict_noun]

        leng = len(terms)
        # f.write(tid+"\t"+str(leng))
        for term in terms:
            f.write(tid+"\t"+str(leng)+"\t"+term+"\n")
            # f.write("\t"+term)
        # f.write("\n")
        count += 1
        if count == 100000:
            break
    f.close()
    tmp_file.close()
    return

