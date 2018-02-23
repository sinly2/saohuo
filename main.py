# -*- coding: utf-8 -*-
"""
Created on Feb 11, 2018

@author: guxiwen
"""

from download import SeleniumDownloader
from util import get_redis,CONFIG
from base import MongoQueue
import threading,time,lxml.html
import json


class Downloader(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.redis_conn = get_redis()
        self.queue = MongoQueue()
        #self.queue.clear()
        #self.queue.push(url,1)

    def run(self):
        while True:
            try:
                url,goods_id,origin_price = self.queue.pop()
                print url,goods_id,origin_price
            except:
                pass
            else:
                with SeleniumDownloader(proxy=None) as browser:
                    #browser.get_page_source(url,goods_id)
                    browser.get_items(url,goods_id,Downloader.parse_func)
            time.sleep(2)


    @staticmethod
    def parse_func(url,page_source,goods_id):
        # 解析网页存到redis给process消费
        redis_conn = get_redis()
        tree = lxml.html.fromstring(page_source)
        divs = tree.cssselect("div.s_result_item")
        result = []
        lowest_price = 0.0
        goods_url = ""
        last_search_url = url
        if divs:
            for div in divs:
                try:
                    # 获取商品地址和价格
                    url = div.attrib["billsrcurl"]
                    price = div.cssselect("div.item_price > em")[0].text_content().encode("utf8").replace("¥","").replace(" ","")
                    if lowest_price == 0.0 or lowest_price > float(price):
                        lowest_price = float(price)
                        goods_url = url
                    result.append({url:price})
                except:
                    pass
            if result:
                result.append({"lowest_price":lowest_price})
            print result
        redis_conn.lpush(CONFIG["redisKey"]["crawledGoods"],json.dumps({"goodsId":goods_id,"goodsUrl":goods_url, "lastSearchUrl":last_search_url,"metadata":result}))
        return result


if __name__ == "__main__":
    # page = ""
    # with open("123.html","r") as f:
    #     for line in f.readlines():
    #         page = page + line.replace("\t"," ")
    # Downloader.parse_func(page.decode("utf8"),1002)
    p = Downloader()
    p.start()