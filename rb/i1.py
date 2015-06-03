# -*- coding:utf-8 -*-

# some improvement after discussion
# use scipy.sparse

import os
import sys
import math
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.base import issparse
from operator import add
sys.path.append('../')
from utils import local2mfs, now
from load_data import load_data_from_mongo, cut_words_local
from config import CLUSTER_NUM, CLUSTERING_ITER, INITIAL_ITER, RB_ITER, convergeDist, FILTER_SCALE
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors

K = 2
# AB_PATH = os.path.dirname(os.path.abspath(__file__))
# print AB_PATH


def parseKV(line):
    tid, leng, term = line.split('\t')
    return ((tid, term), 1.0 / float(leng))

def vector_length(x):
    dot_vec = x.dot(x.transpose())
    result = math.sqrt(dot_vec.data[0])
    return result

def cosine_dist(x, y, x_length, y_length):
    result = 0.0
    numerator = x.dot(y.transpose())
    denominator = x_length * y_length
    result = numerator / denominator
    try:
        res = result.data[0]
    except:
        res = 0
    return res

def closestPoint(p, centers, withDist=False):
    bestIndex = 0
    closest = float("-inf")
    p_length = vector_length(p)
    for i in range(len(centers)):
        tempDist = cosine_dist(p, centers[i][1], p_length, centers[i][2])
        if tempDist > closest:
            closest = tempDist
            bestIndex = i
    if withDist == True:
        return (bestIndex, closest)
    else:
        return  bestIndex

def clustering(doc_vec, K, convergeDist, iter_count_limit):
    kPoints = doc_vec.takeSample(False, K)
    points_length = [0,0]
    tempDist = 0.0
    convergeDist = 2.0
    iter_count = 0
    for i in range(len(kPoints)):
        kPoints[i] = list(kPoints[i])
        kPoints[i].append(vector_length(kPoints[i][1]))

    while tempDist < convergeDist and iter_count < iter_count_limit:
        iter_count += 1

        closest = doc_vec.map(
                lambda (tid, feature):(closestPoint(feature, kPoints), (tid, feature, 1)))
        pointState = closest.reduceByKey(
                lambda (x1, y1, z1), (x2, y2, z2): (-1, y1 + y2, z1 + z2))
        newPoints = pointState.map(
                lambda (x, (flag, y, z)): (x, y / z)).map(
                lambda (x, y): (x, y, vector_length(y))).collect()

        tempDist = sum(cosine_dist(kPoints[x][1], y, kPoints[x][2], z) for (x, y, z) in newPoints)
        for (x, y, z) in newPoints:
            kPoints[x][1] = y
            kPoints[x][2] = z
    return kPoints, tempDist, iter_count

def cluster_evaluation(doc_vec, kPoints):

    size = doc_vec.count()

    closest = doc_vec.map(
            lambda (tid, feature):(closestPoint(feature, kPoints, True), (tid, feature, 1)))
    doc_variance = closest.map(
            lambda ((index, dist), (tid, feature, num)): (index, (dist, num)))
    cluster_variance = doc_variance.reduceByKey(lambda (x1,y1),(x2,y2):(x1+x2,y1+y2))
    # i2 to i1
    i1_cluster_variance = cluster_variance.map(
            lambda (index, (dist, num)):(index, (math.pow(dist, 2)/size, num)))
    total_variance = i1_cluster_variance.map(
            lambda (index, (dist, num)): (dist, num)).reduce(lambda (x1,y1), (x2,y2):(x1+x2,y1+y2))

    return cluster_variance, total_variance

def cal_cluster_variance(doc_vec):
    size = doc_vec.count()
    cluster_center = doc_vec.mapValues(
            lambda x: (x, 1)).values().reduce(
            lambda (y1, z1), (y2, z2): (y1 + y2, z1 + z2))
    _sum, _num = cluster_center
    center = _sum / _num
    center_length = vector_length(center)
    initial_distance = doc_vec.mapValues(
            lambda x:cosine_dist(x, center, vector_length(x), center_length)).values().sum()
    # i2 to i1
    result = math.pow(initial_distance,2)/size
    return result

def load_cut_to_rdd(input_file, result_file, cluster_num=CLUSTER_NUM, clu_iter=CLUSTERING_ITER,\
        ini_iter=INITIAL_ITER, rb_iter=RB_ITER, con_dist=convergeDist, filter_scale=FILTER_SCALE):
    sc = SparkContext(appName='PythonKMeans',master="mesos://219.224.135.91:5050")
    lines = sc.textFile(input_file)
    data = lines.map(parseKV).cache()

    doc_term_tf = data.reduceByKey(add).cache()

    num_doc = doc_term_tf.map(lambda ((tid, term), tf): tid).distinct().count()

    initial_term_idf = doc_term_tf.map(
            lambda ((tid, term), tf): (term, 1.0)
            ).reduceByKey(add)
    # filter
    initial_num_term = initial_term_idf.count()
    print 'initial_num_term', initial_num_term
    idf_sum = initial_term_idf.values().sum()
    print 'idf_sum', idf_sum

    idf_average = idf_sum  / (initial_num_term * filter_scale)
    term_idf = initial_term_idf.filter(
            lambda (term, idf): idf_average < idf < (idf_average * (filter_scale - 1))).mapValues(
            lambda idf: math.log(float(num_doc) / (idf+1)))
    terms_list = term_idf.keys().collect()
    num_term = len(terms_list)
    print 'num_term', num_term

    tfidf_join = doc_term_tf.map(
            lambda ((tid, term), tf): (term, (tid, tf))).join(term_idf)
    tfidf = tfidf_join.map(lambda (term, ((tid, tf), idf)): (tid, (terms_list.index(term), tf*idf)))

    doc_vec = tfidf.groupByKey().mapValues(lambda feature : csr_matrix(Vectors.sparse(num_term, feature).toArray())).cache()
    global_center = doc_vec.mapValues(
            lambda x: x / num_doc).values().reduce(add)
    g_length = vector_length(global_center)

    # initial 2-way clustering
    maximum_total_variance = 0
    best_kPoints = []
    print 'initial', now()
    for i in range(ini_iter):
        kPoints, tempDist, iter_count = clustering(doc_vec, K, con_dist, clu_iter)
        # evaluation
        cluster_variance, total_variance = cluster_evaluation(doc_vec, kPoints)

        # choose the best initial cluster
        if total_variance[0] > maximum_total_variance:
            maximum_total_variance = total_variance[0]
            best_kPoints = kPoints
    # global_distance = sum(cosine_dist(best_kPoints[x][1], global_center, best_kPoints[x][2], g_length) for x in range(len(best_kPoints)))

    f = open(result_file,'w')
    f.write(str(iter_count)+"\t"+str(num_doc)+"\t"+str(num_term)+"\n")
    for index in range(len(terms_list)):
        f.write(terms_list[index].encode('utf-8')+'\t')
    """
    for (term, ((tid,tf), idf)) in tfidf_join.collect():
        f.write(term.encode('utf-8')+'\t'+str(tid)+'\t'+str(tf)+'\t'+str(idf)+'\n')
    print >> f, "%0.9f" % tempDist
    print >> f, "total_variance", total_variance[0], total_variance[1]
    print >> f, "global_dist", global_distance
    f.write("center:"+"\t")
    for dim in global_center:
        f.write(str(dim)+"\t")
    f.write("\n")
    for i in range(len(best_kPoints)):
        f.write(str(i))
        for unit in best_kPoints[i][1]:
            f.write("\t")
            f.write(str(unit))
        f.write("\n")
    for (index, (dist, num)) in cluster_variance.collect():
        f.write(str(index))
        f.write("\t")
        f.write(str(dist))
        f.write("\t")
        f.write(str(num))
        f.write("\n")
    """
    f.close()
    #repeated bisect
    #choose cluster

    updated_dict = {}
    updated_points_dict = {}
    total_delta_variance = 0
    updated_dict[total_delta_variance] = doc_vec
    updated_points_dict[total_delta_variance] = best_kPoints

    print 'repeated', now()
    for j in range(2, cluster_num+1):
        if not (total_delta_variance in updated_dict):
            print "no cluster to divide"
            break

        if total_delta_variance < 0:
            print "no improvement"
            break

        print 'cluster to divide', total_delta_variance, updated_dict[total_delta_variance]
        best_cluster = updated_dict[total_delta_variance]
        global_best_kPoints = updated_points_dict[total_delta_variance]
        del updated_dict[total_delta_variance]
        del updated_points_dict[total_delta_variance]
        closest = best_cluster.map(
                lambda (tid, feature):(closestPoint(feature, global_best_kPoints), (tid, feature))).cache()
        print 'total_count', closest.count()

        total_delta_variance = float("-inf") # clear to zero
        for key in updated_dict:
            if key > total_delta_variance:
                total_delta_variance = key

        for i in range(K):
            single_cluster = closest.filter(lambda (index, (tid, feature)): index == i).values().cache()
            print 'count', i, single_cluster.count()

            maximum_total_variance = 0
            best_kPoints = []
            initial_distance = cal_cluster_variance(single_cluster)
            for j in range(rb_iter):
                # clustering
                kPoints, tempDist, iter_count = clustering(single_cluster, K, con_dist, clu_iter)
                # evaluation
                cluster_variance, total_variance = cluster_evaluation(single_cluster, kPoints)

                if total_variance[0] > maximum_total_variance:
                    maximum_total_variance = total_variance[0]
                    best_kPoints = kPoints

            improvement = maximum_total_variance - initial_distance
            updated_dict[improvement] = single_cluster  # update dict
            updated_points_dict[improvement] = best_kPoints
            print 'improvement', improvement, maximum_total_variance, initial_distance

            if improvement > total_delta_variance:
                total_delta_variance = improvement
                print 'length', cluster_variance.count()

    count = 0
    for key in updated_dict:
        count += 1
        print 'key', key
        per_cluster = updated_dict[key]

        total_similarity = cal_cluster_variance(per_cluster)
        f = open('results/cluster_'+str(count), 'w')
        print >> f, key, total_similarity

        results_list = per_cluster.values().reduce(add).toarray()
        for row in results_list:
            for index in range(len(row)):
                value = row[index]
                if value != 0:
                    f.write('('+str(index)+','+str(value)+')\t')
        f.write('\n')
        for (tid, feature) in per_cluster.collect():
            f.write(tid)
            """
            for row in feature.toarray():
                for unit in range(len(row)):
                    f.write('\t')
                    f.write(str(row[unit]))
            """
            f.write('\n')
        f.close()

    sc.stop()
    return

if __name__ == '__main__':

    # topic = "APEC-微博"
    # print topic
    input_file = "../data/source_chaijing.txt"
    output_file = "../data/out_chaijing2.txt"
    result_file = "results/result_chaijing.txt"
    print "step1", now()
    # load_data_from_mongo(topic, input_file)

    print "step2", now()
    # cut_words_local(input_file, output_file)

    print "step3", now()
    load_cut_to_rdd(local2mfs(output_file), result_file)
    print "end", now()
