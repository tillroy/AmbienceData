# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class NewMexicoSpider(Spider):
    name = u"us_new_mexico"
    source = u"http://drdasnm1.alink.com"
    tz = u"US/Arizona"
    custom_settings = {
        u"DOWNLOAD_DELAY": 2,
    }

    def start_requests(self):
        codes = (u"1", u"4", u"81", u"26", u"103", u"97", u"108", u"8", u"93", u"38", u"35", u"90", u"63", u"72", u"50",
                 u"60", u"42", u"46", u"69", u"66", u"56")
        # codes = (u"97",)

        url = u"http://drdasnm1.alink.com/StationInfo1.aspx?"

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

        pollutant_name, pollutant_units = zip(*pollutant_name_data)

        raw_data = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td')
        raw_data_time = raw_data[0].xpath(u".//span[1]/text()").extract_first()
        data_time = parser.parse(raw_data_time).replace(tzinfo=(timezone(self.tz)))

        raw_pollutant_value = raw_data[1:]

        pollutant_value = [el.xpath(u".//span[1]/@title").extract_first().split(u") ")[1] for el in raw_pollutant_value]

        data = zip(pollutant_name, pollutant_value, pollutant_units)

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
        # self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
