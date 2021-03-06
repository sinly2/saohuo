# -*- coding: utf-8 -*-
"""
Created on Feb 5, 2018

@author: guxiwen
"""
from datetime import datetime, timedelta
from pymongo import MongoClient, errors
from util import get_mongo


class MongoQueue(object):
    OUTSTANDING, PROCESSING, COMPLETE = range(3)

    def __init__(self, client=None, timeout=300):
        self.client = get_mongo() if client is None else client
        self.db = self.client.cache
        self.timeout = timeout

    def __nonzero__(self):
        record = self.db.crawl_queue.find_one(
            {"status": {'$ne': self.COMPLETE}}
        )
        return True if record else False

    def push(self, *args):
         try:
            self.db.crawl_queue.insert({"_id": args[0], "status": self.OUTSTANDING,"goodsId":args[1],"cost":args[2]})
            print 'success...'
         except errors.DuplicateKeyError as e:
             pass
         except Exception as e:
             print e

    def pop(self):
        record = self.db.crawl_queue.find_and_modify(
            query={"status": self.OUTSTANDING},
            update={"$set": {"status": self.PROCESSING, "timestamp": datetime.now()}}
        )
        if record:
            return record["_id"],record["goodsId"],record["cost"]
        else:
            self.repair()
            raise KeyError

    def peek(self):
        record = self.db.crawl_queue.find_one({"status": self.OUTSTANDING})
        if record:
            return record["_id"]

    def complete(self, url):
        #self.db.crawl_queue.update({"_id": url},{"$set": {"status": self.COMPLETE}})
        self.db.crawl_queue.find_one_and_delete({"_id":url})

    def repair(self):
        record = self.db.crawl_queue.find_and_modify(
            query={
                "timestamp": {"$lt": datetime.now()-timedelta(seconds=self.timeout)},
                "status": {"$ne": self.COMPLETE}
            },
            update={"$set": {"status": self.OUTSTANDING}}
        )
        if record:
            print "Released:", record["_id"]

    def clear(self):
        self.db.crawl_queue.drop()