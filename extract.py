# -*- coding:utf-8 -*-

def get_words(terms_list):
    for i in range(1,6):
        f = open('results/cluster_'+str(i),'r')
        f.readline()
        line = f.readline()
        f.close()

        p = open('results/text_'+str(i),'w')
        feature_list = line.strip().split('\t')
        results_list = []
        for feature in feature_list:
            index, value = feature.lstrip('(').rstrip(')').split(',')
            results_list.append((index, float(value)))
        results_list = sorted(results_list, key=lambda word:word[1], reverse=True)
        print results_list[0][0], results_list[0][1]

        count = 0
        for feature in results_list:
            p.write(terms_list[int(feature[0])]+'\t'+str(feature[1])+'\n')
            count += 1
            if count == 100:
                break
        p.close()

def get_texts(text_dict):
    for i in range(1,6):
        f = open('results/cluster_'+str(i),'r')
        f.readline()
        f.readline()
        p = open('results/weibo_'+str(i),'w')
        count = 0
        for line in f.readlines():
            tid = line.strip().split('\t')[0]
            text = text_dict[tid]
            p.write(text+'\n')
            count += 1
            if count == 100:
                break
        p.close()
        f.close()

if __name__ == "__main__":
    f = open('results/result_chaijing.txt','r')
    f.readline()
    line = f.readline()
    f.close()

    terms_list = line.strip().split('\t')
    print len(terms_list)
    # print terms_list[0]
    # print terms_list[-1]
    get_words(terms_list)
    print 'words got'

    f = open('data/source_chaijing.txt','r')
    text_dict = {}
    for line in f.readlines():
        try:
            tid, text = line.split('\t')
        except:
            pass
        text_dict[tid] = text
    f.close()
    get_texts(text_dict)
    print len(text_dict.items())
