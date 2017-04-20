# -*- coding: utf-8

from datetime import datetime

from scrapy import Spider
import pandas as pd

import ujson
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class LouisvilleSpider(Spider):
    name = u"us_louisville_pollution"
    source = u"http://air.ky.gov"
    tz = u"US/Eastern"

    start_urls = (u"https://aaws.louisvilleky.gov/api/v1/Monitor/CityAQI",)

    def get_station_data(self, resp):
        json = ujson.loads(resp.text)

        param = ("ParameterDescription", "Average", "Units", "AverageHour")

        all_records = list()
        for site in json["Sites"]:
            station_id = site["AQSSiteId"]

            st_records = list()
            readings = site.get("Readings")
            if readings is not None:
                for rec in readings:
                    res = {key: rec[key] for key in param}
                    res["station_id"] = station_id

                    st_records.append(res)

                all_records.extend(st_records)

        # print(all_records)
        all_data = pd.DataFrame(all_records)
        current_date_time = all_data["AverageHour"].max()
        current_data = all_data[all_data["AverageHour"] == current_date_time]

        grouped = current_data[["station_id", "ParameterDescription", "Average", "Units"]].groupby(by="station_id")

        for name, gr in grouped:
            # print(name)
            station_data = dict()
            station_id = name
            for record in gr.itertuples(index=False):
                # if station_id is None:
                #     station_id = record[0]

                pollutant_name = record[1]
                pollutant_value = record[2]
                pollutant_units = record[3]

                # print(pollutant_name, pollutant_value, pollutant_units)

                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                pollutant.set_raw_name(pollutant_name)
                pollutant.set_raw_value(pollutant_value)
                pollutant.set_raw_units(pollutant_units)

                # print("Validated", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    station_data[pollutant.get_name()] = pollutant.get_value()

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = parser.parse(current_date_time).replace(tzinfo=timezone(self.tz))
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el