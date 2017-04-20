# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class NevadaSpider(Spider):
    name = u"us_nevada_pollution"
    source = u"http://www.cemp.dri.edu"
    tz = u"US/Pacific"

    def start_requests(self):
        codes = (
            u"alam", u"amar", u"beat", u"boul", u"cali", u"cedu", u"comp", u"delu", u"duck", u"ely1", u"gold", u"hend",
            u"indi", u"lasv", u"medl", u"milu", u"mesq", u"nyal", u"over", u"pahr", u"pioc", u"rach", u"sarc", u"stgu",
            u"ston", u"teco", u"tono", u"twih", u"warm")
        # codes = (u"alam",)

        href = u"http://www.cemp.dri.edu/cgi-bin/cemp_stations.pl?stn={station_id}&change=Go"

        for code_value in codes:
            url = href.format(station_id=code_value)

            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        raw_date = resp.xpath(u"id('col2')/div[1]/center[1]/h3/text()").extract_first()

        raw__all_data = resp.xpath(u"//*[@id='col2']/div[1]/center[2]/table/tr")
        raw_poll_name_p1 = raw__all_data[0].xpath(u"td")[1:]
        raw_poll_name_p2 = raw__all_data[1].xpath(u"td")[1:]
        raw_units = raw__all_data[2].xpath(u"td")[1:]

        poll_name_p1 = list()
        for el in raw_poll_name_p1:
            poll_name = el.xpath(u"./text()").extract()
            if not poll_name:
                poll_name = [u""]

            dup_count = el.xpath(u"./@colspan").extract_first(default=1)
            poll_name_p1.extend(poll_name * int(dup_count))

        poll_name_p1 = [u" ".join(el.split()) for el in poll_name_p1]

        poll_name_p2 = list()
        for el in raw_poll_name_p2:
            poll_name = el.xpath(u"./text()").extract()
            if not poll_name:
                poll_name = [u""]

            dup_count = el.xpath(u"./@colspan").extract_first(default=1)
            poll_name_p2.extend(poll_name * int(dup_count))

        poll_name_p2 = [u" ".join(el.split()) for el in poll_name_p2]

        pollutant_name = map(u" ".join, zip(poll_name_p1, poll_name_p2))
        pollutant_name = [u" ".join(el.split()) for el in pollutant_name]
        pollutant_name = [None if el == u"" else el for el in pollutant_name]

        units = list()
        for el in raw_units:
            unit_name = el.xpath(u"./text()").extract()
            dup_count = el.xpath(u"./@colspan").extract_first(default=1)
            units.extend(unit_name * int(dup_count))

        units = [u" ".join(el.split()) for el in units]
        units = [None if el == u"" else el for el in units]

        # print(units)
        # print(pollutant_name)

        raw_table = raw__all_data[3:-18]
        records = list()
        for el in raw_table:

            col = el.xpath(u"td")
            hour = col[0].xpath(u"center/text()").extract_first()

            # print(hour)

            raw_values = [el.xpath(u"./text()").extract_first(default=u"") for el in col[1:]]
            values = [u" ".join(el.replace(u"\n", u"").split()) for el in raw_values]
            values = [None if el == u"" else el for el in values]

            raw_data_date = u" ".join((raw_date, hour))
            raw_data_date = u" ".join(raw_data_date.split())
            data_time = parser.parse(raw_data_date)

            data = zip(pollutant_name, values, units)
            # print(data)
            for rec in data:
                _rec = {
                    u"name": rec[0],
                    u"value": rec[1],
                    u"unit": rec[2],
                    u"date": data_time,
                }

                records.append(_rec)

        df = pd.DataFrame(records)
        df = df.dropna(axis=0)
        # df.replace(r'\s*', None, regex=True)
        # df.replace(to_replace="", value=None)
        grouped = df.groupby(by="date", as_index=False).count()
        current_date = grouped[grouped["name"] > 1]["date"].max()

        curr_data = df[df[u"date"] == current_date]
        print(curr_data)

        station_data = dict()
        for el in curr_data[[u"name", u"value", u"unit"]].itertuples(index=False):
            pollutant_name = el[0]
            pollutant_value = el[1]
            pollutant_units = el[2]

            # print(pollutant_name, pollutant_value, pollutant_units)

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            pollutant.set_raw_units(pollutant_units)

            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = pd.to_datetime(current_date).replace(tzinfo=timezone(self.tz))
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            return items

