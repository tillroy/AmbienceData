# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class NovaScotiaSpider(Spider):
    name = u"ca_nova_scotia"
    tz = u"Canada/Newfoundland"
    source = u"https://novascotia.ca"

    custom_settings = {
        u"USER_AGENT": u"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36",
    }

    def start_requests(self):
        codes = (u"17", u"3", u"10", u"2", u"9", u"4", u"5", u"8")
        # codes = (u"17",)
        url = u"https://novascotia.ca/nse/airdata/StationInfo3.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(url, u"ST_ID", code_value)
            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        data_time = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td[1]/span/text()').extract_first()
        data_time = parser.parse(data_time)
        #
        pollutant_name = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[1]/td')[1:]
        pollutant_name = [el.xpath(u"span/text()").extract_first() for el in pollutant_name]
        pollutant_data = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td')[1:]
        pollutant_data = [el.xpath(u"span/text()").extract_first() for el in pollutant_data]

        data = zip(pollutant_name, pollutant_data)

        station_data = dict()
        for record in data:
            pollutant = Kind(self.name).get_dict(r_key=record[0], r_val=record[1])
            if pollutant:
                station_data[pollutant[u"key"]] = pollutant[u"val"]

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time.replace(tzinfo=timezone(self.tz))
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
