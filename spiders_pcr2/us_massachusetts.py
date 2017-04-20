# coding: utf-8

import re
from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
import ujson

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class MassachusettsSpider(Spider):
    name = u"us_massachusetts"
    source = u"http://www.mass.gov"
    tz = u"US/Eastern"

    start_urls = (u"http://public.dep.state.ma.us/MassAir/Pages/MapCurrent.aspx?&ht=1&hi=101",)

    def get_station_data(self, resp):
        raw_data_time = resp.xpath(u'//*[@id="ctl00_ContentPlaceHolder2_lbCaption"]/text()').re(u"Current Max Pollution Level \((\d\d?/\d\d?/\d\d\d\d \d\d?..)")[0]
        data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

        res = resp.xpath(u'//*/script/text()').extract()
        script = res[2]
        raw_part = re.findall(u"^.+?function initSiteList\(\)\s+\{(.+?)}\s+/\*", script, re.DOTALL)

        station_datas = re.findall(u"\.params = (\[.+?\]);", raw_part[0])
        station_ids = re.findall(u"new SiteInfo\(\"(.+?)\"", raw_part[0])
        stations = zip(station_ids, station_datas)
        for st in stations:
            station_id = st[0]

            data_json = ujson.loads(st[1])
            data = [(el[u"name"], el[u"val"], el[u"unit"]) for el in data_json]

            station_data = dict()
            for record in data:
                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                # print(record)
                pollutant.set_raw_name(record[0])
                pollutant.set_raw_value(record[1])
                pollutant.set_raw_units(record[2])
                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    station_data[pollutant.get_name()] = pollutant.get_value()

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
