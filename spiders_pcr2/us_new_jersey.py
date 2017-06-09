# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from pytz import timezone
from dateutil import parser

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class NewJerseySpider(Spider):
    name = u"us_new_jersey"
    source = u"http://www.njaqinow.net"
    tz = u"US/Eastern"
    custom_settings = {
        u"DOWNLOAD_DELAY": 2,
    }

    def start_requests(self):
        codes = (u"38", u"16", u"17", u"34", u"2", u"10", u"11", u"30", u"25", u"24", u"12", u"37", u"6", u"36", u"18",
                 u"32")
        # codes = (u"32",)

        url = u"http://www.njaqinow.net/StationInfo.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(url, u"ST_ID", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        raw_name = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[1]/td')[1:]
        pollutant_name = [el.xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0] for el in raw_name]

        raw_units = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[2]/td')[1:]
        units = [el.xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0] for el in raw_units]

        raw_data = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[last()]/td')

        data_time = raw_data[0].xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0]
        data_time = data_time.replace(u"24:00", u"00:00")
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

        raw_pollutant_value = raw_data[1:]
        pollutant_value = [el.xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0] for el in raw_pollutant_value]

        data = zip(pollutant_name, pollutant_value, units)

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

        # print(station_data)
        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
