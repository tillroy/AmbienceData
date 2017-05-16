# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
import ujson

from pollution_app.feature import Feature
from pollution_app.adcrawler import RandomRequest
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ttt(Spider):
    name = u"ttt"
    source = u"http://dec.alaska.gov"
    tz = u"US/Alaska"

    def start_requests(self):
        codes = (u"8", u"20", u"1", u"5", u"10", u"13", u"26", u"17")
        # codes = (u"8",)

        href = u"https://www.arb.ca.gov/aqmis2/display.php?year=2017&mon=5&day=4&param=CO&order=name&hours=morning&county_name=--COUNTY--&basin=--AIR+BASIN--&latitude=--PART+OF+STATE--&o3switch=new&ptype=aqd&report=HVAL&statistic=HVAL&btnsubmit=Update+Display"
        # url = href.format(codes_value)

        yield RandomRequest(
            url=href,
            callback=self.parse,
            # meta={u"code": codes_value}
        )

    def parse(self, response):
        print(response.request.headers)
        print(response.text[:100])
