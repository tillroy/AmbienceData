# coding: utf-8

from datetime import datetime
import re

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
import numpy as np
import pandas as pd
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ZakarTwoSpider(Spider):
    name = u"ua_zakar_two_weather"
    source = u"http://www.gmc.uzhgorod.ua"
    tz = u"EET"

    # custom_settings = {
    #     "ITEM_PIPELINES": {'pollution_app.pipelines.WeatherPipeline': 300}
    # }

    def start_requests(self):
        codes = (u"2", u"8", u"13", u"5", u"26", u"27", u"42", u"48", u"49", u"18", u"19", u"20", u"21", u"22", u"23",
                 u"36", u"12", u"9", u"4", u"10", u"17", u"37", u"39", u"40", u"41", u"34", u"35", u"6", u"14", u"3",
                 u"1", u"28", u"31", u"38", u"15", u"16", u"7", u"33", u"46", u"43", u"44", u"45", u"11", u"32", u"29",
                 u"30", u"25")
        # codes = (u"2",)

        href = u"http://www.gmc.uzhgorod.ua/fixdata1.php?"

        for code_value in codes:
            url = add_or_replace_parameter(href, u"StNo", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        raw_col_names = resp.xpath(u"/html/body/div[1]/div[2]/table/tr[1]/td").extract()
        col_names = [re.sub(u"<.+?>", u"", el) for el in raw_col_names]
        # for el in col_names:
        #     print el

        table = resp.xpath(u'/html/body/div[1]/div[2]/table//td')
        table_data = [el.xpath(u".").re(u"<td>(.+)<\/td>")[0] if el.xpath(u".").re(u"<td>(.+)<\/td>") else None for el
                      in table]
        # print(table_data)
        # print(len(table_data))
        table_data = np.asarray(table_data).reshape(len(table_data)/len(col_names), len(col_names))

        df = pd.DataFrame(table_data[1:, ], columns=col_names)
        # print(df)
        raw_data = df.iloc[0].to_dict()
        raw_data_time = raw_data.pop(u"Дата і час", None)

        data_time = parser.parse(raw_data_time, dayfirst=True).replace(tzinfo=timezone(self.tz))

        data = raw_data

        units = {
            u"Температура повітря": u"degc",
            u"Опади": u"mm",
            u"Рівень №2": u"NA",
            u"Рівень №1": u"NA",
            u"Рівень": u"NA",
            u"Температура води": u"degc",
        }

        station_data = dict()
        for key, val in data.items():
            # print(key)
            poll_name = key
            poll_value = val
            poll_units = units[key]
            # print(poll_name, poll_value, poll_units)

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            # print("record", record)
            pollutant.set_raw_name(poll_name)
            pollutant.set_raw_value(poll_value)
            pollutant.set_raw_units(poll_units)
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



# curl http://localhost:6821/addversion.json -F project=ambiencedata_app -F version=r23 -F egg=/opt/crawler/Weather/wcr1/eggs/ambiencedata_app/1487684742.egg
