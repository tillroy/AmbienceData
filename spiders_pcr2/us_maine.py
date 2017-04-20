# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import url_query_parameter
from dateutil import parser
from pytz import timezone
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class MaineSpider(Spider):
    name = u"us_maine"
    source = u"http://www.maine.gov/"
    tz = u"US/Eastern"

    def start_requests(self):
        yield Request(
            url=u"http://www.maine.gov/dep/air/ozone/hourly_data.html",
            callback=self.get_global_date
        )

    def get_global_date(self, resp):
        date_parts = resp.xpath(u'id("hourly_data_search_form")/form/fieldset/ul/li[2]/select/option[@selected="selected"]/@value').extract()

        params = (
            u"ozone",
            u"pm",
        )

        href = u"http://www.maine.gov/dep/air/ozone/hourly_data.html?type={param}&date_Month={month}&date_Day={day}&date_Year={year}&view=View+Data"
        urls = [href.format(month=date_parts[0], day=date_parts[1], year=date_parts[2], param=el) for el in params]

        return Request(
            url=urls.pop(),
            callback=self.get_station_data,
            meta={
                u"urls": urls,
                u"date_parts": date_parts,
                u"global_data": None
            }
        )

    def get_station_data(self, resp):
        # FIXME add value of DATA HOUR
        try:
            pollutant_unit = {
                u"pm": u"ug/m3",
                u"ozone": u"ppb"
            }
            id_refs = {
                "Cape Elizabeth": "23-005-2003",
                "Kennebunkport": "23-031-2002",
                "McFarland Hill": "23-009-0103",
                "Port Clyde": "23-013-0004",
                "Jonesport": "23-029-0019",
                "Sipayik (Pleasant...": "23-029-0032",
                "Cadillac": "23-009-0102",
                "Bethel": "23-017-0002",
                "Durham": "23-001-0014",
                "Gardiner": "23-011-2005",
                "Shapleigh Ball Park": "23-031-0040",
                "West Buxton": "23-031-0038",
                "Holden": "23-019-4008",
                "Madawaska": "23-003-0014",
                "Presque Isle": "23-003-1008",
                "Carrabassett": "23-007-2002",
                "Rumford": "23-017-2011",
                "Lewiston": "23-001-0011",
                # "Bangor": "23-019-0010",
                # "Bangor": "23-019-0002",
                "Portland Deering ...": "23-005-0029"
            }

            pollutant_name = url_query_parameter(resp.url, u"type")
            date = u"{0}-{1}-{2}".format(*resp.meta[u"date_parts"])

            tables = resp.xpath(u"//*[@id='maincontent2']/table")

            dfs = list()
            for table in tables:
                raw_station_name = table.xpath(u"tr[2]/th")[1:]
                station_names = [el.xpath(u"./text()").extract_first() for el in raw_station_name]
                station_ids = [id_refs.get(el) for el in station_names]

                raw_station_data = table.xpath(u"tr[last()-1]")
                hour = raw_station_data.xpath(u"th[1]/text()").extract_first()
                raw_station_data = raw_station_data.xpath(u"td")
                raw_station_values = [el.xpath(u"./text()").extract_first() for el in raw_station_data]

                raw_data_time = u" ".join((date, hour))
                data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

                station_values = [u"".join(el.split()) if el is not None else el for el in raw_station_values]
                data = zip(station_ids, station_values)
                table_data = [{u"station_id": el[0],
                               u"pollutant_name": pollutant_name,
                               u"pollutant_value": el[1],
                               u"pollutant_unit": pollutant_unit.get(pollutant_name),
                               u"date": data_time} for el in data]
                df = pd.DataFrame(table_data)
                dfs.append(df)

            df_data = pd.concat(dfs, ignore_index=True)

            if resp.meta.get(u"global_data") is not None:
                new_global_data = pd.concat([resp.meta.get(u"global_data"), df_data], ignore_index=True)
            else:
                new_global_data = df_data

            resp.meta[u"global_data"] = new_global_data

            # print(resp.url)

            yield Request(
                url=resp.meta[u"urls"].pop(),
                callback=self.get_station_data,
                meta={
                    u"urls": resp.meta[u"urls"],
                    u"date_parts": resp.meta[u"date_parts"],
                    u"global_data": resp.meta[u"global_data"]
                }

            )
        except IndexError:
            data = resp.meta[u"global_data"]

            current_data_time = data[u"date"].max()
            data = data[data[u"date"] == current_data_time]
            data = data[[u"station_id", u"pollutant_name", u"pollutant_value", u"pollutant_unit"]]

            # print(data)

            grouped = data.groupby(by=u"station_id")

            for name, gr in grouped:
                station_data = dict()
                # print(name)
                station_id = None
                for record in gr.itertuples(index=False):
                    if station_id is None:
                        station_id = record[0]

                    pollutant_name = record[1]
                    pollutant_value = record[2]
                    pollutant_units = record[3]

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
                if station_data:
                    items = AppItem()
                    items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                    items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
                    items[u"data_value"] = station_data
                    items[u"source"] = self.source
                    items[u"source_id"] = station_id

                    yield items
