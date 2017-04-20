# coding: utf-8
from datetime import datetime

from scrapy import Spider
from dateutil import parser
from pytz import timezone
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class WisconsinSpider(Spider):
    name = u"us_wisconsin"
    source = u"http://dnr.wi.gov"
    tz = u"US/Central"

    start_urls = (u"https://dnrx.wisconsin.gov/wisards/webreports/browseDailyContData.do",)

    def get_station_data(self, resp):
        raw_date = resp.xpath(u"/html/body/table[3]/tr/td/table/tr/td/table[2]/tr/td/form/table/tr[1]/td/text()").extract()[1]
        raw_date = u" ".join(raw_date.split())
        curr_date = parser.parse(raw_date)
        str_date = u"{dd}-{mm}-{yyyy}".format(dd=curr_date.day, mm=curr_date.month, yyyy=curr_date.year)
        # print(curr_date)

        col_names = resp.xpath(u"/html/body/table[3]/tr/td/table/tr/td/table[2]/tr/td/form/table/tr[last()]/td[1]/table/tr[1]/th")[1:-1]
        col_names = [el.xpath(u"./text()").extract_first() for el in col_names]
        # print(col_names)

        table = resp.xpath(u"/html/body/table[3]/tr/td/table/tr/td/table[2]/tr/td/form/table/tr[last()]/td[1]/table/tr[@id]")
        df_records = list()
        for row in table:
            record = row.xpath(u"./td")[1:-1]
            record = [el.xpath(u"./text()").extract_first() for el in record]

            data = zip(col_names, record)
            df_record = dict(data)
            df_records.append(df_record)

        df = pd.DataFrame(df_records, columns=col_names)
        # could be one value in column
        df = df.dropna(axis=1, thresh=1)
        df = df.dropna(axis=0, how=u'all')

        latest_data = df[[u"Site", u"Param", u"UNITS", df.columns[-1]]]

        hour = latest_data.columns[-1]
        raw_data_time = u"{0} {1}:00".format(str_date, hour)
        data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

        grouped = latest_data.groupby(by=u"Site")

        for name, gr in grouped:

            station_data = dict()
            station_id = None
            for record in gr.itertuples(index=False):
                if station_id is None:
                    station_id = record[0]

                pollutant_name = record[1]
                pollutant_value = record[3]
                pollutant_units = record[2]

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
            # print(name)

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el