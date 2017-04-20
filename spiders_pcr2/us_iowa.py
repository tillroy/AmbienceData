# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone
import pandas as pd
from io import StringIO

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class IowaSpider(Spider):
    name = u"us_iowa"
    source = u"http://www.shl.uiowa.edu"
    tz = u"US/Central"

    def start_requests(self):
        codes = (u"2", u"4", u"3", u"7", u"50", u"6", u"15", u"20", u"54", u"8", u"12", u"55", u"13", u"47", u"10",
                 u"5", u"53", u"27", u"46", u"44")
        # codes = (u"53",)
        # 46 27 53 13 55 12 8 6 2 15 4 50 20 3

        href = u"http://www.shl.uiowa.edu/Application/airquality/RT.cgi?csv=1"
        for code_value in codes:
            url = add_or_replace_parameter(href, u"site", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        # print(resp.text)
        all_data = pd.read_csv(
            StringIO(resp.text),
            names=(u"station_name", u"str_date", u"pollutant_name", u"pollutant value")
        ).dropna(axis=0)
        all_data[u"date_time"] = [parser.parse(x) for x in all_data[u'str_date']]

        current_data_time = all_data[u"date_time"].max()
        # print(current_data_time)
        curr_all_data = all_data[all_data[u"date_time"] == current_data_time]

        idx = curr_all_data.groupby(by=u"pollutant_name")[u"date_time"].transform(max) == curr_all_data[u"date_time"]

        units = {
            u"PM2.5": u"ug/m3",
            u"PM10-NEW": u"ug/m3",
            u"Sulfur Dioxide": u"ppb",
            u"Carbon Monoxide": u"ppb",
            u"Ozone 1 hour": u"ppb",
            u"Nitrogen Dioxide": u"ppb",
        }

        data = curr_all_data[idx].copy()

        station_data = dict()
        for el in data.itertuples():
            pollutant_name = el[3]
            pollutant_value = el[4]
            pollutant_units = units.get(pollutant_name)

            # print(pollutant_name, pollutant_value, pollutant_units)

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            pollutant.set_raw_units(pollutant_units)
            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

        # print(station_data)
        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        # self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
