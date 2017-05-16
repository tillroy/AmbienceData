# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class LousianaSpider(Spider):
    name = u"us_lousiana"
    source = u"http://louisiana.gov"
    tz = u"US/Central"

    def start_requests(self):
        yield Request(
            url=u"http://airquality.deq.louisiana.gov",
            callback=self.get_global_date
        )

    def get_global_date(self, resp):
        date_time = resp.xpath(u"//*[@id='main']/div/ul/li[1]/div/p[2]/span[3]/text()").extract_first()
        date_time = parser.parse(date_time).replace(tzinfo=timezone(self.tz))
        codes = (u"DIXIE", u"SHREVEPORT", u"MONROE", u"CARVILLE", u"CONVENT", u"DUTCHTOWN", u"FRENCHSETTLEMENT",
                 u"BAYOUPLAQUEMINE", u"LSU", u"CAPITOL", u"PORTALLEN", u"SOUTHERN", u"PRIDE", u"NEWROADS", u"I610",
                 u"KENNER", u"THIBODAUX", u"CHALMETTEVISTA", u"GARYVILLE", u"CITYPARK", u"MERAUX", u"MADISONVILLE",
                 u"LAFAYETTE", u"STMARTINVILLE", u"CARLYSS", u"VINTON", u"WESTLAKE", u"LIGHTHOUSE")
        # codes = (u"DIXIE",)

        href = u"http://airquality.deq.louisiana.gov/Data/Site/{station_id}/Date/{mm}-{dd}-{yyyy}"
        for code_value in codes:
            url = href.format(dd=date_time.day, mm=date_time.month, yyyy=date_time.year, station_id=code_value)
            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={u"code": code_value, u"date_time": date_time}
            )

    def get_station_data(self, resp):
        raw_poll_name = resp.xpath(u'//*[@id="main"]/div[1]/div[1]/table/thead/tr/th')[1:]
        poll_name = [el.xpath(u"abbr/text()").extract_first() for el in raw_poll_name]
        poll_unit = [el.xpath(u"span/abbr/text()").extract_first() for el in raw_poll_name]

        raw_poll_value = resp.xpath(u"//*[@id='main']/div[1]/div[1]/table/tbody/tr[last()]/td")[1:]
        poll_value = [el.xpath(u"text()").extract_first() for el in raw_poll_value]

        data = zip(poll_name, poll_value, poll_unit)

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
            items[u"data_time"] = resp.meta[u"date_time"]
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items
