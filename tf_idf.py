# -*- coding:utf-8 -*-

# use cosine_distance

import os
import re
import math
import numpy as np
from operator import add
from utils import local2mfs, now
from load_data import load_data_from_mongo, cut_words_local
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors

AB_PATH = os.path.dirname(os.path.abspath(__file__))
print AB_PATH

def parseKV(line):
    tid, leng, term = line.split('\t')
    return ((tid, term), 1.0 / float(leng))

def load_cut_to_rdd(input_file, result_file):
    sc = SparkContext(appName='PythonKMeans',master="mesos://219.224.135.91:5050")
    lines = sc.textFile(input_file)
    data = lines.map(parseKV).cache()

    doc_term_tf = data.reduceByKey(add).cache()

    num_doc = doc_term_tf.map(lambda ((tid, term), tf): tid).distinct().count()
    terms_list = doc_term_tf.map(lambda ((tid, term), tf): term).distinct().collect()
    num_term = len(terms_list)

    term_idf = doc_term_tf.map(
            lambda ((tid, term), tf): (term, 1.0)
            ).reduceByKey(add).mapValues(lambda idf: math.log(float(num_doc) / (idf+1)))
    tfidf_join = doc_term_tf.map(
            lambda ((tid, term), tf): (term, (tid, tf))).join(term_idf)
    tfidf = tfidf_join.map(lambda (term, ((tid, tf), idf)): (tid, (terms_list.index(term), tf*idf)))

    doc_vec = tfidf.groupByKey().mapValues(lambda feature : Vectors.sparse(num_term, feature).toArray()).cache()

    nonzero_count = 0
    f = open(result_file,'w')
    f.write('%s %s\r\n'%(num_doc, num_term))
    for (tid, feature) in doc_vec.collect():
        for num in feature:
            f.write(str(num)+"\t")
        f.write("\n")
    f.close()
    sc.stop()


    return

def cluto_kmeans_vcluster(k=5, input_file=None, vcluster=None):
    '''
    cluto kmeans聚类
    输入数据：
        k: 聚簇数
        input_file: cluto输入文件路径，如果不指定，以cluto_input_folder + pid.txt方式命名
        vcluster: cluto vcluster可执行文件路径

    输出数据：
        cluto聚类结果, list
        聚类结果评价文件位置及名称
    '''
    # 聚类结果文件, clustering_file

    cluto_input_folder = './cluto'

    evaluation_file = os.path.join(cluto_input_folder,'evaluation_%s.txt'% k)
    clustering_file = '%s.clustering.%s' % (input_file, k)

    if not vcluster:
        vcluster = '../cluto-2.1.1/Linux/vcluster'

    command = "%s -niter=20 %s %s > %s" % (vcluster, input_file, k, evaluation_file)
    os.popen(command)

    # 提取聚类结果
    results = [line.strip() for line in open(clustering_file)]

    #提取每类聚类效果
    with open(evaluation_file) as f:
        s = f.read()
        pattern = re.compile(r'\[I2=(\S+?)\]')
        res = pattern.search(s).groups()
        evaluation_results = res[0]

    """
    if os.path.isfile(clustering_file):
        os.remove(clustering_file)

    if os.path.isfile(input_file):
        os.remove(input_file)

    if os.path.isfile(evaluation_file):
        os.remove(evaluation_file)
    """

    return results, evaluation_results

if __name__ == "__main__":
    # topic = "APEC-微博"
    # print topic
    input_file = "data/source_chaijing.txt"
    output_file = "data/out_chaijing.txt"
    result_file = "chaijing"
    print "step1", now()
    # load_data_from_mongo(topic, input_file)

    print "step2", now()
    # cut_words_local(input_file, output_file)

    print "step3", now()
    load_cut_to_rdd(local2mfs(output_file), result_file)
    cluto_kmeans_vcluster(5, result_file)
    print "end", now()
