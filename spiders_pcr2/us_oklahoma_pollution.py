# -*- coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
import pandas as pd

from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class OklahomaSpider(Spider):
    name = u"us_oklahoma_pollution"
    tz = u"US/Central"
    source = u"http://www.deq.state.ok.us"

    def start_requests(self):
        codes = (u"0604", u"0217", u"0167", u"0188", u"0170", u"0101", u"0049", u"0144", u"1073", u"0033", u"0096",
                 u"0415", u"0178", u"0175", u"0035", u"0235", u"0001", u"0174", u"0004", u"0006", u"0860", u"0651",
                 u"1037", u"0179", u"1110", u"0856", u"0065", u"1127", u"0097", u"0226", u"0297", u"0555", u"0671",
                 u"0300", u"0380", u"0416")
        # codes = (u"0380",)

        href = u"http://www.deq.state.ok.us/aqdnew/monitoring/webdata/Site{station_id}.htm"

        for code_value in codes:
            url = href.format(station_id=code_value)

            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={u"code": code_value}
            )

    @staticmethod
    def data_to_df(resp):
        col_names = (u"name", u"value", u"unit", u"time")

        blocks = resp.xpath(u'//table')

        all_data = list()
        for block in blocks:
            date = block.xpath(u".//td[@class='csE28D1D5B']/nobr/text()").extract_first()

            raw_pollution_names = block.xpath(u'./tr[td[@class="cs180237E4"] and not(td[@class="cs3F5174F9"]) and not(td[@class="csDB0246A3"])]/td/nobr')
            pollution_names = [n.xpath(u"./text()").extract_first() for n in raw_pollution_names]

            count = len(pollution_names)

            raw_pollutant_units = block.xpath(u'./tr[td[@class="cs180237E4"] and not(td[@class="cs3F5174F9"]) and td[@class="csDB0246A3"]]/td[not(@class="csDB0246A3")]/nobr')
            pollutant_units = [n.xpath(u"./text()").extract_first() for n in raw_pollutant_units]


            all_data_records = block.xpath(u'./tr[td[@class="cs3F5174F9"]]')

            part_data = list()
            for rec in all_data_records:
                hour = rec.xpath(u"./td[@class='cs180237E4']/nobr/text()").extract_first()

                raw_date_time = " ".join((date, hour))
                data_time = parser.parse(raw_date_time)

                raw_values = rec.xpath(u"./td[@class='cs3F5174F9']")
                values = [v.xpath(u"./nobr/text()").extract_first() for v in raw_values]

                # print(pollution_names, values, pollutant_units, [data_time] * count)
                rec_data = zip(pollution_names, values, pollutant_units, [data_time] * count)

                res = [dict(zip(col_names, el)) for el in rec_data]
                part_data.extend(res)

            all_data.extend(part_data)
            # print(all_data)
            # print(pollution_names)
            # print(pollutant_units)

        df = pd.DataFrame(all_data)

        return df

    def get_station_data(self, resp):
        data = self.data_to_df(resp)
        data = data.dropna(axis=0)
        # print(data)

        current_data_time = data[u"time"].max()
        current_data = data[data[u"time"] == current_data_time]

        station_data = dict()
        for record in current_data[[u"name", u"value", u"unit"]].itertuples(index=False):

            pollutant_name = record[0]
            pollutant_value = record[1]
            pollutant_units = record[2]

            # print(station_id, pollutant_name, pollutant_value, pollutant_units)

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            pollutant.set_raw_units(pollutant_units)
    #
            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta["code"]

            yield items
