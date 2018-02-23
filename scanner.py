# -*- coding: utf-8 -*-
"""
Created on Feb 13, 2018

@author: guxiwen
"""
from util import get_db,get_time,CONFIG
from urllib import quote
from base import MongoQueue
import time


class Scanner(object):
    def __init__(self):
        self.redis_conn = get_db("REDIS")
        self.db_conn = get_db("SQLITE")
        self.queue = MongoQueue()
        self.search_url = CONFIG["basic"]["searchUrlFormat"]

    def run(self):
        while True:
            now_time = get_time()
            search_url = self.search_url
            sql = 'select id,keywords,originPrice from goods where nextCrawlTime is null or nextCrawlTime<="%s" ' % now_time
            print sql
            try:
                cursor = self.db_conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                if rows:
                    for row in rows:
                        goods_id = row[0]
                        keywords = row[1]
                        origin_price = row[2]
                        if keywords.strip() == "" or keywords is None:
                            continue
                        else:
                            search_url = self.generate_url(keywords,origin_price)
                            print search_url
                            try:
                                self.queue.push(search_url, goods_id,origin_price)
                            except Exception as e:
                                print e
                                continue
                    print rows
            except Exception as e:
                print e
                continue
            finally:
                cursor.close()
                time.sleep(10)

    def generate_url(self,keywords,origin_price):
        search_url = self.search_url
        search_url = search_url.replace("#keyword",quote(keywords.encode("utf8")))
        search_url = search_url.replace("#price1", str(int(origin_price)/3))
        search_url = search_url.replace("#price2", str(int(origin_price)))
        return search_url


if __name__ == "__main__":
    s = Scanner()
    s.run()
