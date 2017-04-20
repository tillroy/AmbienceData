# -*- coding: utf-8

from datetime import datetime

from scrapy import Spider
import pandas as pd

from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class OhioSpider(Spider):
    name = u"us_ohio_pollution"
    source = u"http://epa.ohio.gov"
    tz = u"US/Eastern"

    start_urls = (u"http://epa.ohio.gov/portals/27/airohio/reports/paramnow.htm",)

    def get_date(self, resp):
        raw_date = resp.xpath(u"/html/body/p[2]/text()").re(u"last updated on (\d+.\d+.\d+)")[0]
        return raw_date

    def get_station_data(self, resp):

        raw_data_time = self.get_date(resp)

        table = resp.xpath(u"/html/body/div[1]/left/table/tbody/tr")
        # print(table)
        # hours = table.pop(0).xpath(u"td")[2:-3]
        col_names = table.pop(0).xpath(u"td")
        col_names = [el.xpath(u"p/b/text()").extract_first() for el in col_names]

        global_pollutant_name = None

        data = list()
        for row in table:
            col = row.xpath(u"td")

            pollutant_name = col.pop(0).xpath(u"tt/text()").extract_first()
            if pollutant_name:
                global_pollutant_name = pollutant_name

            pollutant_name = global_pollutant_name if pollutant_name is None else pollutant_name

            row_values = [el.xpath(u"tt/text()").extract_first() for el in col]
            row_values.insert(0, pollutant_name)

            data.append(row_values)

        df = pd.DataFrame(data, columns=col_names)
        df = df.dropna(thresh=1, axis=1)
        # df = df.dropna(thresh=1, axis=0)
        del_cols = list(df.columns.values)
        df.drop(labels=del_cols[-3:], axis=1, inplace=True)

        del_mid_cols = list(df.columns.values)
        df.drop(labels=del_mid_cols[2:-1], axis=1, inplace=True)
        # print(df)

        units = {
            u"o3": u"ppb",
            u"pm25": u"ug/m3",
            u"pm10": u"ug/m3",
            u"co": u"ppm",
            u"so2": u"ppb",
            u"no2": u"ppb",
        }

        grouped = df.groupby(by=u"Site Name")
        for name, gr in grouped:
            station_id = name
            hour = list(gr.columns.values)[-1]
            data_time = "{0} {1}:00".format(raw_data_time, hour)
            data_time = parser.parse(data_time)

            station_data = dict()
            for rec in gr.itertuples(index=False):
                pollutant_name = rec[0]
                pollutant_value = rec[2]

                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                pollutant.set_raw_name(pollutant_name)
                pollutant.set_raw_value(pollutant_value)
                try:
                    pollutant.set_raw_units(units[pollutant.get_name()])
                except KeyError:
                    print(
                        "There is no such pollutant in local units list <<<<<<<<{0}>>>>>>".format(pollutant.get_name()))

                # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())

                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    station_data[pollutant.get_name()] = pollutant.get_value()

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time.replace(tzinfo=timezone(self.tz))
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el