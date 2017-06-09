# coding: utf-8
from datetime import datetime
from pytz import timezone

from scrapy import Spider, Request
import pandas as pd
from dateutil import parser

from pollution_app import rextension
from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class LondonSpider(Spider):
    name = u"gb_london"
    tz = u"GMT"
    source = u"http://www.londonair.org.uk"

    def start_requests(self):
        codes = (
            u"EN7", u"EN1", u"BW1", u"HR1", u"EN4", u"EN5", u"HG1", u"HR2", u"HG4", u"RB4", u"HV3", u"RB7", u"BG1",
            u"IS6", u"IS2", u"CP2", u"BT5", u"BT4", u"CD1", u"TH5", u"BT6", u"MW4", u"EA6", u"BG2", u"CD9", u"HK6",
            u"EI1", u"MY1", u"MY7", u"TH2", u"BL0", u"KC7", u"KC1", u"MW1", u"HV1", u"CT4", u"EA8", u"EI8", u"TK3",
            u"IM1", u"CD3", u"TH4", u"CT2", u"WM6", u"CT3", u"NB1", u"BX3", u"CT6", u"CT8", u"EA7", u"EI7", u"HF4",
            u"KC3", u"WM9", u"WM8", u"HI0", u"KC2", u"WM0", u"BQ7", u"BQ8", u"SK6", u"GN2", u"BX2", u"BX0", u"GN0",
            u"KC5", u"TH6", u"LH0", u"KC4", u"GN3", u"GR8", u"LB5", u"LW3", u"SK5", u"RI1", u"MW2", u"WAA", u"TK8",
            u"TK1", u"RI2", u"LW2", u"GR7", u"BX1", u"BX9", u"WA9", u"LW4", u"LB4", u"WA8", u"WAC", u"WA7", u"TK4",
            u"WA2", u"GR9", u"GB0", u"GB6", u"RHG", u"RD0", u"GR4", u"LW1", u"GN4", u"WAB", u"LB6", u"TD5", u"TD0",
            u"ME2", u"KT3", u"CR5", u"CR8", u"ME1", u"ME7", u"ST5", u"ST8", u"KT4", u"ST6", u"CR9", u"ST3", u"CR7",
            u"ST4", u"ZV3", u"ZV1", u"ZV2", u"RG1", u"RG2", u"RG6", u"RG3"
        )

        now = datetime.now()
        today = rextension.time_to_dict(now, 0)
        yesterday = rextension.time_to_dict(now, 1)

        for code_value in codes:
            url = u"http://api.erg.kcl.ac.uk/AirQuality/Data/Site/SiteCode={code}/StartDate={y_year}-{y_month}-{y_day}/EndDate={t_year}-{t_month}-{t_day}". format(
                y_year=yesterday[u"year"],
                y_month=yesterday[u"month"],
                y_day=yesterday[u"day"],
                t_year=today[u"year"],
                t_month=today[u"month"],
                t_day=today[u"day"],
                code=code_value
            )

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        source_code = resp.meta[u"code"]

        data_list = resp.xpath(u"/html/body/airqualitydata/data")

        table_frame = list()
        for data in data_list:
            pollutant = data.xpath(u"@speciescode").extract()[0]
            date = data.xpath(u"@measurementdategmt").extract()[0]
            value = data.xpath(u"@value").extract()[0]
            res = {
                u"pollutant": pollutant,
                u"date": parser.parse(date),
                u"value": value
            }
            table_frame.append(res)

        df = pd.DataFrame(table_frame, columns=(u"pollutant", u"value", u"date"))

        for date in df[u"date"].unique():
            res = df[df[u"date"] == date]
            data_time = pd.to_datetime(date)
            data_time = data_time.replace(tzinfo=timezone(self.tz))

            station_data = dict()
            for el in res.itertuples():
                pollutant = Kind(self.name).get_dict(r_key=el.pollutant, r_val=el.value)
                if pollutant:
                    station_data[pollutant[u"key"]] = pollutant[u"val"]

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = source_code

                yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
