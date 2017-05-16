# coding: utf-8

from datetime import datetime
import re

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from pollution_app.rextension import clean


class HawaiiSpider(Spider):
    name = u"us_hawaii"
    source = u"http://emdweb.doh.hawaii.gov/air-quality"
    tz = u"US/Hawaii"

    def start_requests(self):
        codes = (u"49", u"34", u"44", u"46", u"37", u"60", u"45", u"54", u"41", u"36", u"38", u"40", u"48", u"42")
        # codes = (u"49",)

        url = u"http://emdweb.doh.hawaii.gov/air-quality/StationInfo.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(url, u"ST_ID", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        raw_poll_name = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[1]/td')[1:].extract()
        poll_name = [re.findall(u"\r\n\t(.+)\r\n", re.sub(u"<.+?>", u"", el))[0] for el in raw_poll_name]

        raw_poll_unit = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[2]/td')[1:].extract()
        poll_unit = [re.findall(u"\r\n\t(.+)\r\n", re.sub(u"<.+?>", u"", el))[0] for el in raw_poll_unit]

        raw_data = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[last()]/td')
        data_time = raw_data[0].xpath(u'.//div[1]/text()').extract_first()
        data_time = data_time.replace(u"24:00", u"00:00")
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))
        # print(data_time)

        raw_pollutant_value = raw_data[1:]
        pollutant_value = list()
        for el in raw_pollutant_value:
            value = el.xpath(u'.//div/text()').extract_first()
            value = clean(value)
            if u"\xa0" in value:
                value = None
            pollutant_value.append(value)

        data = zip(poll_name, pollutant_value, poll_unit)
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
