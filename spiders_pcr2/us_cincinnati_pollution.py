# -*- coding: utf-8

from datetime import datetime, timedelta

from scrapy import Spider, Request
import pandas as pd
import numpy as np
from w3lib.url import add_or_replace_parameter

import ujson
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class CincinnatiSpider(Spider):
    name = u"us_cincinnati_pollution"
    tz = u"US/Eastern"
    source = u"http://www.southwestohioair.org"

    def start_requests(self):
        all_codes = [u"1", u"5", u"4", u"9"]
        init_codes = u"2"

        href = "http://hamiltoncounty.sonomatechdata.com/frames/chart/trendsData.json?paramId={param_id}&selectedDate={mm}%2F{dd}%2F{yyyy}&hours=1&siteIDs=23041%2C6601%2C23679%2C2187%2C2154%2C6068%2C23644%2C22985%2C2186%2C2192%2C23047&"

        now = datetime.now(timezone(self.tz))

        url = href.format(
            param_id=init_codes,
            mm=now.month,
            dd=now.day,
            yyyy=now.year,
        )

        yield Request(
            url=url,
            callback=self.get_station_data,
            meta={
                u"code": init_codes,
                u"all_codes": all_codes,
                u"res_df": list(),
                u"href": url
            }
        )

    def get_station_data(self, resp):
        try:
            pollutant_name = {
            u"2": u"PM25",
            u"1": u"Ozone",
            u"5": u"CO",
            u"4": u"NO2",
            u"9": u"SO2"
        }

            pollutant_unit = {
                u"2": u"ug/m3",
                u"1": u"ppb",
                u"5": u"ppm",
                u"4": u"ppb",
                u"9": u"ppb"
            }

            json = ujson.loads(resp.text)
            one_pollutant_data = list()
            for station in json:
                raw_station_name = station[0]
                station_name = raw_station_name["siteName"]

                raw_data = station[1]
                station_data = pd.DataFrame(raw_data)
                station_data[u"date"] = [parser.parse(x) for x in station_data[u'date']]
                station_data[u"station_name"] = station_name
                station_data[u"pollutant_name"] = pollutant_name.get(resp.meta[u"code"])
                if "hexColor" in station_data.columns.values:
                    del station_data[u"hexColor"]

                station_data[u"unit"] = pollutant_unit.get(resp.meta[u"code"])

                one_pollutant_data.append(station_data)

            one_pollutant_data = pd.concat(one_pollutant_data, ignore_index=True)

            res_def = resp.meta["res_df"]
            res_def.append(one_pollutant_data)

            new_code = resp.meta[u"all_codes"].pop()
            url = add_or_replace_parameter(resp.meta["href"], "paramId", new_code)

            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={
                    u"code": new_code,
                    u"all_codes": resp.meta[u"all_codes"],
                    u"res_df": res_def,
                    u"href": url
                }
            )

            # print(one_pollutant_data)
        except IndexError:
            all_data = pd.concat(resp.meta["res_df"], ignore_index=True)
            all_data[all_data["aqi"] == -999.0] = np.nan
            all_data = all_data.dropna(axis=0)

            current_date = all_data["date"].max() - timedelta(hours=2)
            current_data = all_data[all_data["date"] == current_date]

            grouped = current_data[["pollutant_name", "aqi", "unit", "station_name"]].groupby(by="station_name")
            # print(grouped)

            for name, gr in grouped:
                station_id = name

                station_data = dict()
                for rec in gr.itertuples(index=False):
                    pollutant_name = rec[0]
                    pollutant_value = rec[1]
                    pollutant_unit = rec[2]

                    # print(pollutant_name, pollutant_value, pollutant_unit)

                    pollutant = Feature(self.name)
                    pollutant.set_source(self.source)

                    pollutant.set_raw_name(pollutant_name)
                    pollutant.set_raw_value(pollutant_value)
                    pollutant.set_raw_units(pollutant_unit)

                    # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())

                    if pollutant.get_name() is not None and pollutant.get_value() is not None:
                        station_data[pollutant.get_name()] = pollutant.get_value()
                #
                if station_data:
                    items = AppItem()
                    items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                    items[u"data_time"] = pd.to_datetime(current_date).replace(tzinfo=timezone(self.tz))
                    items[u"data_value"] = station_data
                    items[u"source"] = self.source
                    items[u"source_id"] = station_id

                    yield items



