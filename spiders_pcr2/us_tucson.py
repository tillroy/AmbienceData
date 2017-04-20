# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from pytz import timezone
from dateutil import parser

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class TucsonSpider(Spider):
    name = u"us_tucson"
    source = u"http://envista.pima.gov"
    tz = u"US/Pacific"

    def start_requests(self):
        codes = (u"9", u"12", u"2", u"10", u"4", u"11", u"3", u"6", u"13", u"7", u"8", u"5", u"15")
        # codes = (u"4",)

        url = u"http://envista.pima.gov/StationInfo1.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(url, u"ST_ID", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        raw_pollutant_name = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[1]/td')[1:]
        pollutant_name_data = [el.xpath(u".//span[1]/@title").extract_first().split(u"\n\n") for el in raw_pollutant_name]
        pollutant_name = [el[0] for el in pollutant_name_data]
        pollutant_units = [el[1] for el in pollutant_name_data]

        pollutant_name = map(lambda x: u" ".join(x.split()), pollutant_name)

        raw_data = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td')

        data_time = raw_data[0].xpath(u".//span[1]/text()").extract_first()
        data_time = data_time.replace(u"24:00", u"00:00")
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

        raw_pollutant_value = raw_data[1:]
        pollutant_value = [el.xpath(u'.//span[1]/text()').extract_first() for el in raw_pollutant_value]

        data = zip(pollutant_name, pollutant_value, pollutant_units)

        # print(data)

        station_data = dict()
        for record in data:
            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            # print("record", record)
            pollutant.set_raw_name(record[0])
            pollutant.set_raw_value(record[1])
            pollutant.set_raw_units(record[2])
            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

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
