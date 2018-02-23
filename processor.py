# -*- coding: utf-8 -*-
"""
Created on Feb 12, 2018

@author: guxiwen
"""
from util import get_db,CONFIG,get_time
from base import MongoQueue
import json


class Processor(object):
    def __init__(self):
        self.redis_conn = get_db("REDIS")
        self.db_conn = get_db("SQLITE")
        self.queue = MongoQueue()

    def run(self):
        # 从redis中获取消息解析，没有消息的话一直阻塞
        while True:
            try:
                # timeout为0，没有key时会无限阻塞
                data = self.redis_conn.brpop(CONFIG["redisKey"]["crawledGoods"])
            except Exception as e:
                print e
                continue
            data = json.loads(data[1])
            print data
            # if data["metadata"]:
            goods_id = data["goodsId"]
            metadata = data["metadata"]
            lowest_price = data["metadata"][-1]["lowest_price"] if data["metadata"] else 0.0
            goods_url = data["goodsUrl"]
            last_search_url = data["lastSearchUrl"]
            now_time = get_time()
            next_crawl_time = get_time(CONFIG["basic"]["crawlInterval"])
            sql = 'update goods set metaData="%s",lastSearchUrl="%s",goodsUrl="%s",lowestPrice=%s,updateTime="%s",nextCrawlTime="%s" ' \
                  'where id=%d' %(str(metadata),str(last_search_url),goods_url,lowest_price,now_time,next_crawl_time,goods_id)
            #print sql
            self.db_conn.execute(sql)
            self.db_conn.commit()
            self.queue.complete(last_search_url)


if __name__ == "__main__":
    p = Processor()
    p.run()