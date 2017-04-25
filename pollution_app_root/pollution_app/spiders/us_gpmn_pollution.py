# -*- coding: utf-8 -*-

import re
from datetime import datetime
from pytz import timezone

from scrapy import Spider, Request
from dateutil import parser

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class GpmnSpider(Spider):
    name = u"us_gpmn_weather"
    source = u"https://www.nps.gov"
    tz = u""

    def start_requests(self):
        codes = (u"BIBE-KB",)

        href = u"https://nature.nps.gov/air/data/current/data_{station_id}.cfm"

        for code_value in codes:
            url = href.format(station_id=code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    @staticmethod
    def get_name_and_unit(raw_str):
        is_unit = re.findall(".*\(.*", raw_str)

        if len(is_unit) == 0:
            return raw_str, False
        else:
            name = re.findall("(.+) \(", raw_str)[0]
            unit = re.findall(".*\((.+?)\)", raw_str)[0]
            return name, unit

    def get_station_data(self, resp):
        # //*[@id="datatable"]/tbody/tr[1]/th

        raw_data = resp.xpath(u"//*[@id='datatable'][last()]/tr[last()]/td")

        raw_date = resp.xpath(u"//*[@id='datatable'][last()]/tr[1]/th/text()").extract_first()
        date = " ".join(raw_date.split(" ")[-4:])

        raw_col_names = resp.xpath(u"//*[@id='datatable'][last()]/tr[2]/th")

        poll_values = [el.xpath(u"./text()").extract_first() for el in raw_data]
        raw_poll_names = [self.get_name_and_unit(" ".join(el.xpath(u"./text()").extract())) for el in raw_col_names]
        poll_names = [el[0] for el in raw_poll_names]
        poll_units = [el[1] for el in raw_poll_names]

        data = zip(poll_names, poll_values, poll_units)

        hour = data.pop(0)
        hour = hour[1]

        data_time = " ".join((date, hour))
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))
        print(data_time)

        units = {
            u"wd": u"cardinals",
        }

        station_data = dict()
        # for el in data:
        #     pollutant_name = el[0]
        #     pollutant_value = el[1]
        #     pollutant_units = el[2]
        #
        #     pollutant = Feature(self.name)
        #     pollutant.set_source(self.source)
        #     pollutant.set_raw_name(pollutant_name)
        #     pollutant.set_raw_value(pollutant_value)
        #
        #     if pollutant_units is not False:
        #         pollutant.set_raw_units(pollutant_units)
        #     else:
        #         try:
        #             pollutant.set_raw_units(units[pollutant.get_name()])
        #         except KeyError:
        #             print(
        #             u"There is no such pollutant in local units list <<<<<<<<{0}>>>>>>".format(
        #                 pollutant.get_name()))
        #
        #
        #     # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
        #     if pollutant.get_name() is not None and pollutant.get_value() is not None:
        #         station_data[pollutant.get_name()] = pollutant.get_value()
        #
        # if station_data:
        #     items = AppItem()
        #     items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
        #     # items[u"data_time"] = data_time
        #     items[u"data_value"] = station_data
        #     items[u"source"] = self.source
        #     items[u"source_id"] = resp.meta[u"code"]
        #
        #     yield items

    def parse(self, response):
        self.get_station_data(response)
        # for el in self.get_station_data(response):
        #     yield el


