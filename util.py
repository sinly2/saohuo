# -*- coding: utf-8 -*-
"""
Created on Feb 11, 2018

@author: guxiwen
"""
import json
import redis,sqlite3,time
from pymongo import MongoClient
from datetime import timedelta

global CONFIG
REDIS_POOL = None

with open("config.json","r") as f:
    CONFIG = json.load(f)


def get_redis_pool():
    global REDIS_POOL
    if not REDIS_POOL:
        REDIS_POOL = redis.ConnectionPool(host=CONFIG["redis"]["host"],port=CONFIG["redis"]["port"],db=CONFIG["redis"]["db"])
    return REDIS_POOL


def get_redis():
    return redis.Redis(connection_pool=get_redis_pool())


def get_mongo():
    return MongoClient(CONFIG["mongo"]["host"],CONFIG["mongo"]["port"])


def get_db(db_type):
    if db_type.upper() == "SQLITE":
        return sqlite3.connect(CONFIG["database"]["host"])
    elif db_type.upper() == "REDIS":
        return redis.Redis(connection_pool=get_redis_pool())
    elif db_type.upper() == "MONGODB":
        return MongoClient(CONFIG["mongo"]["host"], CONFIG["mongo"]["port"])


def get_time(delay=None):
    if delay:
        if type(delay) not in [int, float]:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 3600))
        else:
            return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()+float(delay)))
    else:
        return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))


if __name__ == "__main__":
    print get_time(100)