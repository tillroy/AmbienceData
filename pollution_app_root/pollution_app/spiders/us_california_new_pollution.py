# coding: utf-8

import re
from datetime import datetime, timedelta

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter, urljoin, url_query_parameter
import pandas as pd
from dateutil import parser
from pytz import timezone

from pollution_app.adcrawler import RandomRequest

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from scrapy.exceptions import CloseSpider

class CaliforniaNewSpider(Spider):
    name = u"us_california_new_pollution"
    source = u"https://www.arb.ca.gov"
    tz = u"US/Pacific"

    request_counter = 0

    def start_requests(self):
        today_in_tz = datetime.now(timezone(self.tz))
        # d = timedelta(days=2)
        # today_in_tz += d

        # href = u"https://www.arb.ca.gov/aqmis2/display.php?year={year}&mon={month}&day={day}&param={param}&order=basin,county_name,name&county_name=--COUNTY--&basin=--AIR+BASIN--&latitude=--PART+OF+STATE--&o3switch=new&ptype=aqd&report=HVAL&statistic=HVAL&btnsubmit=Update+Display&units=007&hours=all"
        href = u"https://www.arb.ca.gov/aqmis2/display.php?year=x&mon=x&day=x&param=&order=basin,county_name,name&county_name=--COUNTY--&basin=--AIR+BASIN--&latitude=--PART+OF+STATE--&o3switch=new&ptype=aqd&report=HVAL&statistic=HVAL&btnsubmit=Update+Display&units=007&hours=all"
        params = ["BENZENE", "BC", "CO", "CO2", "COH", "H2S", "LTSC", "CH4", "NO2", "NO", "NOX",
                  "NOY", "NMHC", "OZONE_ppm", "SO2", "THC", "PMTEOM", "PMBAM", "PM10_LHR",
                  "PM10_SHR", "PM25HR"
                  ]
        # params = ["BENZENE", "OZONE", "CO", "SO2"]
        # params = ["OZONE", "CO", "SO2"]
        # params = ["COH", "H2S",]
        # params = ["H2S",]

        url = add_or_replace_parameter(href, "year", today_in_tz.year)
        url = add_or_replace_parameter(url, "mon", today_in_tz.month)
        url = add_or_replace_parameter(url, "day", today_in_tz.day)

        param = params.pop(0)

        yield RandomRequest(
            # url=href.format(param=params.pop(0)),
            url=add_or_replace_parameter(url, "param", param),
            # callback=self.collect_station_data,
            callback=self.check_validity,
            meta={
                "params": params,
                "data": list(),
                "changed_today_in_tz": None,
                "href": url,
                "param": param
            }
        )

    def get_date_from_url(self, url):
        year = url_query_parameter(url, "year")
        month = url_query_parameter(url, "mon")
        day = url_query_parameter(url, "day")

        # url_date = datetime(year=int(year), month=int(month), day=int(day), tzinfo=timezone(self.tz))
        url_date = datetime(year=int(year), month=int(month), day=int(day))

        return url_date

    def check_validity(self, resp):
        # print("params", resp.meta["params"])
        checker = resp.xpath('//*[@id="content_area"]/center[2]/table[2]/tr[1]/td[2]/text()').extract()
        checker = " ".join("".join(checker).split())
        # print(url_query_parameter(resp.url, "param"))

        if self.request_counter < 2:
            # print("LESS THAN 2 REQUESTS")
            if "No Data Available. Please try your query again." in checker:
                # print("NOT AVALIABLE", resp.meta["data"])

                self.request_counter += 1
                # print("NOT AVAILABLE {0}".format(self.request_counter))
                today_in_tz = self.get_date_from_url(resp.url) - timedelta(days=1)

                url = add_or_replace_parameter(resp.url, "year", today_in_tz.year)
                url = add_or_replace_parameter(url, "mon", today_in_tz.month)
                url = add_or_replace_parameter(url, "day", today_in_tz.day)

                yield RandomRequest(
                    url=url,
                    # callback=self.collect_station_data,
                    callback=self.check_validity,
                    meta={
                        "params": resp.meta["params"],
                        "data": resp.meta["data"],
                        "changed_today_in_tz": True,
                        "href": resp.meta["href"],
                        "param": resp.meta["param"]
                    },
                    dont_filter=True,
                )
            else:
                # print("3", resp.meta["data"])
                self.request_counter = 0

                # print("SUCCESS")

                url = resp.url if resp.meta.get("changed_today_in_tz") is not None else resp.meta["href"]

                yield RandomRequest(
                    url=add_or_replace_parameter(url, "param", resp.meta["param"]),
                    # callback=self.collect_station_data,
                    callback=self.collect_station_data,
                    meta={
                        "params": resp.meta["params"],
                        "data": resp.meta["data"],
                        "href": resp.meta["href"],
                        "param": resp.meta["param"]
                    },
                    dont_filter=True,

                )
        else:
            self.request_counter = 0
            # print("1", resp.meta["data"])

            if len(resp.meta["params"]) != 0:
                param = resp.meta["params"].pop(0)

                yield RandomRequest(
                    # url=href.format(param=params.pop(0)),
                    url=add_or_replace_parameter(resp.meta["href"], "param", param),
                    # callback=self.collect_station_data,
                    callback=self.check_validity,
                    meta={
                        "params": resp.meta["params"],
                        "data": resp.meta["data"],
                        "href": resp.meta["href"],
                        "param": param
                    }
                )
            else:
                self.push_data(resp)
                # for el in self.push_data(resp):
                #     yield

    def get_max_valid_date(self, df):
        time_group = df.groupby(by="time").size().to_frame("size")
        time_group.reset_index(level=0, inplace=True)

        current_time = time_group[time_group["size"] > 40]["time"].max()
        return current_time

    def push_data(self, resp):
        # print("PUSH!!!!!!!!!!!!!!!!!!!!!!")
        data = resp.meta["data"]

        df = pd.DataFrame(data)

        # print(df.groupby(by="name").size())


        df["value"] = pd.to_numeric(df["value"])
        df = df[pd.notnull(df["value"])]

        # print(df.groupby(by="time").size())

        current_time = self.get_max_valid_date(df)
        # print(current_time)
        current_data = df[df["time"] == current_time]
        # print(current_data)
        grouped = current_data.groupby(by="station_id")

        # FIXME check units validity!!!!

        # for station_id, gr in grouped:
        #     station_data = dict()
        #     for poll in gr[["name", "value", "unit"]].itertuples(index=False):
        #         pollutant_name = poll[0]
        #         pollutant_value = poll[1]
        #         pollutant_units = poll[2]
        #
        #         # print(pollutant_name, pollutant_value, pollutant_units)
        #
        #         pollutant = Feature(self.name)
        #         pollutant.set_source(self.source)
        #         pollutant.set_raw_name(pollutant_name)
        #         pollutant.set_raw_value(pollutant_value)
        #         pollutant.set_raw_units(pollutant_units)
        #
        #         # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
        #         if pollutant.get_name() is not None and pollutant.get_value() is not None:
        #             station_data[pollutant.get_name()] = pollutant.get_value()
        #
        #     if station_data:
        #         items = AppItem()
        #         items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
        #         items[u"data_time"] = pd.to_datetime(current_time).replace(tzinfo=timezone(self.tz))
        #         items[u"data_value"] = station_data
        #         items[u"source"] = self.source
        #         items[u"source_id"] = station_id
        #
        #         yield items

    def collect_station_data(self, resp):
        try:
            pollutant_name = url_query_parameter(resp.url, "param")
            # print("Name", pollutant_name)
            # print("Is chaged", resp.meta.get("changed_today_in_tz"))

            date = self.get_date_from_url(resp.url)

            pollutant_unit = resp.xpath('//*[@id="graph"]/table//tr[1]/td/span/text()').extract()[-1]

            raw_table = resp.xpath('//*[@id="graph"]/table//tr')[2:]
            table_column_names = raw_table.pop(0).xpath('.//td/div/text()').extract()[5:]
            hours = ["".join(el.split()).split("-")[1] for el in table_column_names]
            # print(hours)

            table = list()
            for row in raw_table:
                records = row.xpath(".//td")[2:-1]
                station_id = url_query_parameter(records.pop(0).xpath(".//a/@href").extract_first(), "site")
                values = [el.xpath(".//span/text()").extract_first() for el in records[2:]]

                data = zip(values, hours)

                table_record = [{
                    "station_id": station_id,
                    "name": pollutant_name,
                    "value": el[0],
                    "unit": pollutant_unit,
                    "time": date.replace(hour=int(el[1]))
                } for el in data]

                table.extend(table_record)

            resp.meta["data"].extend(table)

            param = resp.meta["params"].pop(0)
            url = add_or_replace_parameter(resp.url, "param", param)

            # print(resp.meta["params"])

            yield RandomRequest(
                url=url,
                callback=self.check_validity,
                meta={
                    "params": resp.meta["params"],
                    "data": resp.meta["data"],
                    "href": resp.meta["href"],
                    "changed_today_in_tz": None if resp.meta.get("changed_today_in_tz") is None else resp.meta.get("changed_today_in_tz"),
                    "param": param
                }
            )

        except IndexError:
            self.push_data(resp)
            # for el in self.push_data(resp):
            #     yield
