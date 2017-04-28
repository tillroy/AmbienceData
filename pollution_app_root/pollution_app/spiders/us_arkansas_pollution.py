# coding: utf-8

from datetime import datetime
import re

from scrapy import Spider, Request, Selector
from dateutil import parser
from pytz import timezone
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ArkansasSpider(Spider):
    name = u"us_arkansas_pollution"
    source = u"https://www.adeq.state.ar.us"
    tz = u"US/Central"

    def start_requests(self):
        params = (
            u"ozone",
            u"pm",
        )

        href = u"https://www.adeq.state.ar.us/techsvs/air_chem_lab/{pollutant_id}_monitors.aspx"
        urls = [href.format(pollutant_id=el) for el in params]

        yield Request(
            url=urls.pop(),
            callback=self.get_station_data,
            meta={
                u"urls": urls,
                u"global_data": None
            }
        )

    def get_station_data(self, resp):
        try:
            pollutant_unit = {
                u"pm": u"ug/m3",
                u"ozone": u"ppb"
            }

            pollutant_name = resp.url.split(u"/")[-1].replace(u"_monitors.aspx", u"")
            # print(pollutant_name)

            # raw_station_names = resp.xpath(u"//*[@id='tblGrid']/thead/tr[1]/td")
            raw_table = resp.xpath(u"//*[@id='tblGrid'][1]").extract_first()
            raw_table = re.sub(u"</?tbody>", u"", raw_table)
            raw_table = re.sub(u"</?thead>", u"", raw_table)
            raw_table = re.sub(u"</th>", u"</td>", raw_table)
            raw_table = re.sub(u"<th ", u"<td ", raw_table)

            table = Selector(text=raw_table)
            # print(table)
            raw_station_names = table.xpath(u"//tr[1]/td")[1:]
            # print(raw_station_names)

            station_names = [u" ".join(el.xpath(u"./b/text()").extract()) for el in raw_station_names]
            station_names = [u" ".join(el.split()) for el in station_names]
            # print(station_names)

            raw_poll_data = table.xpath(u"//*[@id='tblGrid'][1]/tr[last()]/td")

            raw_hour = raw_poll_data[0].xpath(u"./text()").extract_first()
            try:
                raw_date = resp.xpath(u"//*[@id='mainContent']/div[1]/p[2]/text()").re(u"(\d\d?/\d\d?/\d\d\d\d)")[0]
            except IndexError:
                raw_date = None

            # print(raw_date)
            raw_data_time = u" ".join((raw_date, raw_hour))
            data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

            raw_poll_value = raw_poll_data[1:]
            poll_values = [el.xpath(u"font/text()").extract_first() for el in raw_poll_value]

            data = zip(station_names, poll_values)

            table_data = [{u"station_id": el[0],
                           u"pollutant_name": pollutant_name,
                           u"pollutant_value": el[1],
                           u"pollutant_unit": pollutant_unit.get(pollutant_name),
                           u"date": data_time
                           } for el in data]

            df = pd.DataFrame(table_data)

            if resp.meta.get(u"global_data") is not None:
                new_global_data = pd.concat([resp.meta.get(u"global_data"), df], ignore_index=True)
            else:
                new_global_data = df

            # print(new_global_data)
            resp.meta[u"global_data"] = new_global_data

            yield Request(
                url=resp.meta[u"urls"].pop(),
                callback=self.get_station_data,
                meta={
                    u"urls": resp.meta[u"urls"],
                    u"global_data": resp.meta[u"global_data"]
                }
            )

        except IndexError:
            # pass
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