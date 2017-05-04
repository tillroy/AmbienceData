# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class MontanaSpider(Spider):
    name = u"us_montana"
    source = u"http://mt.gov"
    tz = u"US/Mountain"

    def start_requests(self):
        yield Request(
            url=u"http://svc.mt.gov/deq/todaysair/",
            callback=self.get_global_date
        )

    def get_global_date(self, resp):
        raw_date = resp.xpath(u'//*[@id="webbodyheader"]/div[2]/h4[2]/text()').re(u"\d\d?/\d\d?/\d\d\d\d")
        try:
            cor_date = parser.parse(raw_date[0])
        except IndexError:
            cor_date = None

        href = u"http://svc.mt.gov/deq/todaysair/AirDataDisplay.aspx?siteAcronym={station_id}&targetDate={mm}/{dd}/{yyyy}"
        codes = (u"SL", u"BI", u"BH", u"BD", u"BN", u"FV", u"FT", u"OP", u"PS", u"RP", u"LB", u"ML", u"MS", u"SE",
                 u"OF", u"TF", u"PE", u"LT")
        # codes = (u"SL",)
        for code_value in codes:
            url = href.format(station_id=code_value, mm=cor_date.month, dd=cor_date.day, yyyy=cor_date.year)
            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={u"code": code_value, u"date": cor_date}
            )

    def get_station_data(self, resp):
        raw_record = resp.xpath(u"//*[@id='ContentPlaceHolder1_tablebody']/tr[last()]/td")
        hour = raw_record[0].xpath(u"./text()").re(u"-(\d?\d:\d\d)")[0]

        pollutant_value = raw_record[1].xpath(u"./text()").extract_first()
        # only one pollutant
        pollutant_name = u"PM25"
        pollutant_units = u"µg/m"

        cor_date = resp.meta["date"].strftime("%d-%m-%Y")
        raw_data_time = " ".join((cor_date, hour))
        date_time = parser.parse(raw_data_time, dayfirst=True).replace(tzinfo=timezone(self.tz))

        # print(date_time, hour, pollutant_value)

        station_data = dict()

        pollutant = Feature(self.name)
        pollutant.set_source(self.source)
        # print(record)
        pollutant.set_raw_name(pollutant_name)
        pollutant.set_raw_value(pollutant_value)
        pollutant.set_raw_units(pollutant_units)

        if pollutant.get_name() is not None and pollutant.get_value() is not None:
            station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = date_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

