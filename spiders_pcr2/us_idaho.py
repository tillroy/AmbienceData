# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class IdahoSpider(Spider):
    name = u"us_idaho"
    source = u"http://airquality.deq.idaho.gov"
    tz = u"US/Pacific"

    def start_requests(self):
        codes = (u"102", u"114", u"170", u"135", u"96", u"181", u"208", u"210", u"18", u"207", u"11", u"39", u"14",
                 u"30", u"108", u"163", u"195", u"134", u"180", u"36")
        # codes = (u"102",)

        href = u"http://airquality.deq.idaho.gov/StationInfo1.aspx?"

        for code_value in codes:
            url = add_or_replace_parameter(href, u"ST_ID", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        date_time = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td[1]/span/text()').extract_first()
        date_time = date_time.replace(u"24:00", u"00:00")
        data_time = parser.parse(date_time).replace(tzinfo=timezone(self.tz))

        pollutant_name_data = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[1]/td')[1:]
        pollutant_name_data = [el.xpath(u"span/text()").extract() for el in pollutant_name_data]

        pollutant_name = [el[0] for el in pollutant_name_data]
        pollutant_unit = [el[1] for el in pollutant_name_data]

        pollutant_value = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td')[1:]
        pollutant_value = [el.xpath(u"span/text()").extract_first() for el in pollutant_value]

        data = zip(pollutant_name, pollutant_value, pollutant_unit)

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
