# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
import numpy as np
import pandas as pd
import re
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ZakarOneSpider(Spider):
    name = u"ua_zakar_one_weather"
    source = u"http://www.gmc.uzhgorod.ua"
    tz = u"EET"

    def start_requests(self):
        codes = (u"33631", u"33514", u"33634", u"33515", u"33638", u"33633", u"33647", u"33517", u"33518")
        # codes = (u"33631",)

        href = u"http://www.gmc.uzhgorod.ua/metdata1.php?"

        for code_value in codes:
            url = add_or_replace_parameter(href, u"StNo", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        raw_col_names = resp.xpath(u'//*[@id="content1"]/div[1]/table/tr[1]/td').extract()
        col_names = [re.sub(u"<.+?>", u"", el) for el in raw_col_names]
        # for el in col_names:
        #     print el

        table = resp.xpath(u'//*[@id="content1"]/div[1]/table//td')

        table_data = [el.xpath(u".").re(u"<td>(.+)<\/td>")[0] if el.xpath(u".").re(u"<td>(.+)<\/td>") else None for el in table]
        table_data = np.asarray(table_data).reshape(len(table_data)/len(col_names), len(col_names))

        df = pd.DataFrame(table_data[1:,], columns=col_names)
        df[u"Опади"] = df[u"Опади"].apply(lambda x: re.search(u"(.+) \s*\(", x).group(1) if x is not None else 0)

        raw_data = df.iloc[0].to_dict()
        raw_data_time = raw_data.pop(u"Дата і час", None)

        data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

        data = raw_data
        units = {
            u"Температура повітря": u"degc",
            u"Температура точки роси": u"degc",
            u"Опади": u"mm",
            u"Атмосферний тиск": u"mbar",
            u"Напрямок вітру": u"deg",
            u"Швидкість вітру": u"ms",
        }

        station_data = dict()
        for key, val in data.items():
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

