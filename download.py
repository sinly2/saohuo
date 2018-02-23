# -*- coding: utf-8 -*-
"""
Created on Feb 11, 2018

@author: guxiwen
"""
import platform
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
import logging


class SeleniumDownloader(object):
    headers = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36'
        }
    ]

    def __init__(self, proxy=None):
        # 设置代理
        self.proxy = proxy

    def __enter__(self):
        # 打开界面
        self.display = self.get_display()
        #  打开浏览器
        self.browser = self.get_browser(self.proxy)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 关闭浏览器
        try:
            if self.browser:
                self.browser.delete_all_cookies()
                self.browser.quit()
        except Exception as e:
            logging.exception(e)
        # 关闭界面
        try:
            # 关闭浏览器,关闭窗口
            self.display and self.display.stop()
        except Exception as e:
            logging.exception(e)

    def get_display(self):
        """ 获取操作系统桌面窗口 """
        if platform.system() != 'Darwin':
            # 不是mac系统, 启动窗口
            display = Display(visible=0, size=(1024, 768))
            display.start()
        else:
            display = None
        return display

    def get_browser(self, proxy):
        """ 启动并返回浏览器，使用firefox """
        # 启动浏览器
        firefox_profile = webdriver.FirefoxProfile()
        if proxy:
            myProxy = '%s:%s' % (proxy.host, proxy.port)
            ff_proxy = Proxy({
                'proxyType': ProxyType.MANUAL,
                'httpProxy': myProxy,
                'ftpProxy': myProxy,
                'sslProxy': myProxy,
                'noProxy': ''})

            browser = webdriver.Firefox(firefox_profile=firefox_profile, proxy=ff_proxy)
        else:
            browser = webdriver.Firefox(firefox_profile=firefox_profile)
        return browser

    def get_page_source(self,url):
        browser = self.browser
        browser.get(url)
        return browser.page_source

    def get_items(self, url, goods_id,func):
        try:
            page_source = self.get_page_source(url)
            func(url,page_source,goods_id)
        except Exception as e:
            print e

