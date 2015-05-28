# -*- coding:utf-8 -*-

# use SparseVector

import os
import math
import numpy as np
from operator import add
from utils import local2mfs, now
from load_data import load_data_from_mongo, cut_words_local
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors

AB_PATH = os.path.dirname(os.path.abspath(__file__))
print AB_PATH

def cosine_svec(sv1, sv2):
    numerator = sv1.dot(sv2)
    denominator = math.sqrt(sv1.dot(sv1)) * math.sqrt(sv2.dot(sv2))
    result = numerator / denominator
    return result

def add_svec(sv1, sv2):
    assert len(sv1) == len(sv2), "dimension mismatch"
    indices = []
    values = []
    i, j = 0, 0
    while i < len(sv1.indices) and j < len(sv2.indices):
        if sv1.indices[i] == sv2.indices[j]:
            indices.append(sv1.indices[i])
            values.append(sv1.values[i] + sv2.values[j])
            i += 1
            j += 1
        elif sv1.indices[i] < sv2.indices[j]:
            indices.append(sv1.indices[i])
            values.append(sv1.values[i])
            i += 1
        else:
            indices.append(sv2.indices[j])
            values.append(sv2.values[j])
            j += 1
    while i < len(sv1.indices):
        indices.append(sv1.indices[i])
        values.append(sv1.values[i])
        i += 1
    while j < len(sv2.indices):
        indices.append(sv2.indices[j])
        values.append(sv2.values[j])
        j += 1
    return Vectors.sparse(len(sv1), indices, values)

def div_svec(sv, scale):
    indices = sv.indices
    values = []
    for val in sv.values:
        values.append(val / scale)
    return Vectors.sparse(len(sv), indices, values)

def parseKV(line):
    tid, leng, term = line.split('\t')
    return ((tid, term), 1.0 / float(leng))

def closestPoint(p, centers, withDist=False):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = cosine_svec(p,centers[i][1])
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    if withDist == True:
        return (bestIndex, closest)
    else:
        return  bestIndex

def load_cut_to_rdd(input_file, result_file):
    sc = SparkContext(appName='PythonKMeans',master="mesos://219.224.135.91:5050")
    lines = sc.textFile(input_file)
    data = lines.map(parseKV).cache()

    # total_document_count = data.count()
    # print 'total_document_count', total_document_count

    doc_term_tf = data.reduceByKey(add).cache()

    num_doc = doc_term_tf.map(lambda ((tid, term), tf): tid).distinct().count()
    terms_list = doc_term_tf.map(lambda ((tid, term), tf): term).distinct().collect()
    num_term = len(terms_list)

    term_idf = doc_term_tf.map(
            lambda ((tid, term), tf): (term, 1.0)
            ).reduceByKey(add).mapValues(lambda idf: math.log((idf+1) / float(num_doc)))
    tfidf_join = doc_term_tf.map(
            lambda ((tid, term), tf): (term, (tid, tf))).join(term_idf)
    tfidf = tfidf_join.map(lambda (term, ((tid, tf), idf)): (tid, (terms_list.index(term), tf*idf)))

    doc_vec = tfidf.groupByKey().mapValues(lambda feature : Vectors.sparse(num_term, feature)).cache()

    K = 5
    convergeDist = 0.00001
    kPoints = doc_vec.takeSample(False, K)
    for i in range(len(kPoints)):
        kPoints[i] = list(kPoints[i])
    tempDist = 5.0
    iter_count = 0

    while tempDist > convergeDist and iter_count < 10:
        iter_count += 1

        closest = doc_vec.map(
                lambda (tid, feature):(closestPoint(feature, kPoints), (tid, feature, 1)))
        pointState = closest.reduceByKey(
                lambda (x1, y1, z1), (x2, y2, z2): (-1, add_svec(y1, y2), z1 + z2))
        newPoints = pointState.map(
                lambda (x, (flag, y, z)): (x, div_svec(y, z))).collect()

        tempDist = sum(cosine_svec(y, kPoints[x][1]) for (x, y) in newPoints)

        for (x, y) in newPoints:
            kPoints[x][1] = y
    
    # evaluation
    closest = doc_vec.map(
            lambda (tid, feature):(closestPoint(feature, kPoints, True), (tid, feature, 1)))
    doc_variance = closest.map(
            lambda ((index, dist), (tid, feature, num)): (index, (dist, num)))
    cluster_variance = doc_variance.reduceByKey(lambda (x1,y1),(x2,y2):(x1+x2,y1+y2))
    total_variance = cluster_variance.map(
            lambda (index, (dist, num)): (dist, num)).reduce(lambda (x1,y1), (x2,y2):(x1+x2,y1+y2))
    
    global_center = doc_vec.mapValues(
            lambda x: div_svec(x, num_doc)).reduce(
            lambda (x1, y1), (x2, y2): (-1, add_svec(y1, y2)))

    global_distance = sum(cosine_svec(global_center[1], kPoints[x][1]) for x in range(len(kPoints)))
    
    f = open(result_file,'w')
    f.write(str(iter_count)+"\t"+str(num_doc)+"\t"+str(num_term)+"\n")
    print >> f, "%0.9f" % tempDist
    print >> f, "total_variance", total_variance[0], total_variance[1]
    print >> f, "global_dist", global_distance
    f.write("center:"+"\t")
    for dim in global_center[1].toArray():
        f.write(str(dim)+"\t")
    f.write("\n")
    for i in range(len(kPoints)):
        f.write(str(i))
        for unit in kPoints[i][1].toArray():
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
    f.close()

    sc.stop()
    return


if __name__ == "__main__":
    topic = "APEC-微博"
    print topic
    input_file = "data/source_APEC.txt"
    output_file = "data/out_APEC.txt"
    result_file = "data/result_APEC.txt"
    print "step1", now()
    load_data_from_mongo(topic, input_file)

    print "step2", now()
    cut_words_local(input_file, output_file)

    print "step3", now()
    load_cut_to_rdd(local2mfs(output_file), result_file)
    print "end", now()
