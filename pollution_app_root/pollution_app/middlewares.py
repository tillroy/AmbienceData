#  coding: utf-8

from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from .settings import WEBKIT_DOWNLOADER


class WebkitDownloader(object):
    @staticmethod
    def process_request(request, spider):
        if spider.name in WEBKIT_DOWNLOADER:
            driver = webdriver.PhantomJS()
            driver.get(request.url)
            html = driver.page_source.encode('utf-8')
            # close tab
            # driver.close()

            # quit from browser
            driver.quit()
            return HtmlResponse(request.url, encoding='utf-8', body=html, headers=request.headers)


class JSDownloader(object):
    @staticmethod
    def process_request(request, spider):
        # TODO use Splash instead of PhantomJS
        driver = webdriver.PhantomJS()
        driver.get(request.url)
        html = driver.page_source.encode('utf-8')
        # close tab
        # driver.close()

        # quit from browser
        driver.quit()
        return HtmlResponse(request.url, encoding='utf-8', body=html)


class CetesbDownloader(object):
    def process_response(self, request, response, spider):
        if spider.name == 'br_cetesb':
            driver = webdriver.PhantomJS()
            driver.get(request.url)

            select = Select(driver.find_element_by_id("selEst"))
            select.select_by_value(request.meta['select'])

            # select.select_by_value('3/Mooca')
            # select.select_by_index(index)
            # select.select_by_visible_text("text")

            driver.find_element_by_name("ar").click()
            html = driver.page_source.encode('utf-8')
            driver.quit()

            return HtmlResponse(request.url, encoding='utf-8', body=html)
