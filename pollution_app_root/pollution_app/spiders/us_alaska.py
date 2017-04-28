# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request, Selector
from dateutil import parser
from pytz import timezone
import ujson

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class AlaskaSpider(Spider):
    name = u"us_alaska"
    source = u"http://dec.alaska.gov"
    tz = u"US/Alaska"

    def start_requests(self):
        codes = (u"8", u"20", u"1", u"5", u"10", u"13", u"26", u"17")
        # codes = (u"8",)

        href = u"http://dec.alaska.gov/Applications/Air/airtoolsweb/Aq/Station/{0}"
        for codes_value in codes:
            url = href.format(codes_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": codes_value}
            )

    def get_station_data(self, resp):
        raw_text = resp.xpath(u'/html/head/script[last()]/text()')
        raw_pollutant_data = raw_text.re(u"var data = (.+);")
        raw_pollutant_data = [el.replace(u"'", u'"').replace(u"],]", u"]]") for el in raw_pollutant_data]
        pollutant_data = [ujson.loads(el) for el in raw_pollutant_data]
        pollutant_data = [el[-1] for el in pollutant_data]

        pollutant_value = [el[1] for el in pollutant_data]

        # pollutant_date = [el[0] for el in pollutant_data]
        pollutant_date = [parser.parse(el[0]).replace(tzinfo=timezone(self.tz)) for el in pollutant_data]

        # max value as current date
        current_data_time = max(pollutant_date)
        # print(current_data_time)
        # data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

        raw_pollution_name = raw_text.re(u"series: \[\s+\{\s+name: '(.+)',?")
        pollutant_name = [Selector(text=el).xpath(u"/html/body/p/text()").re(u"(.+)\(")[0] for el in raw_pollution_name]
        pollutant_units = [Selector(text=el).xpath(u"/html/body/p/text()").re(u"\((.+)\)")[0] for el in raw_pollution_name]

        data = zip(pollutant_name, pollutant_value, pollutant_units, pollutant_date)

        station_data = dict()
        for record in data:
            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            # print("record", record)
            pollutant.set_raw_name(record[0])
            pollutant.set_raw_value(record[1])
            pollutant.set_raw_units(record[2])
            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None and record[3] == current_data_time:
                station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = current_data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
