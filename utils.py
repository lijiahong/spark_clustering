# -*- coding:utf-8 -*-

import os
import shutil
import pymongo
import time
from config import MONGODB_HOST, MONGODB_PORT, MONGODB_NAME, EVENTS_COLLECTION,\
        EVENTS_COMMENTS_COLLECTION_PREFIX


def local2mfs(input_file):
    #input_file should be absolute path
    prefix_name = '/mnt/mfs'
    shutil.copy(input_file, prefix_name)

    file_name = input_file.split('/')[-1]
    tmp_file_path = os.path.join("file://"+prefix_name, file_name)
    return tmp_file_path


def _default_mongo(host=MONGODB_HOST, port=MONGODB_PORT, usedb=MONGODB_NAME):
    connection = pymongo.MongoClient(host=host, port=port, j=True, w=1)
    db = connection.admin
    # db.authenticate('root', 'root')
    db = getattr(connection, usedb)
    return db

def getDataByName(mongo, name, events_collection=EVENTS_COLLECTION):
    topic_col = mongo[events_collection].find_one({"topic": name})
    if topic_col:
        topic_id = topic_col['_id']
        print topic_id
        results = mongo[EVENTS_COMMENTS_COLLECTION_PREFIX+str(topic_id)].find()
        return [r for r in results]
    else:
        print "no this topic"
        return []

def now():
    return time.strftime('%H:%M:%S', time.localtime(time.time()))

