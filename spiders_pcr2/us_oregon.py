# coding: utf-8
from datetime import datetime

from scrapy import Spider, Selector
from dateutil import parser
import ujson
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class OregonSpider(Spider):
    name = u"us_oregon"
    tz = u"US/Pacific"
    source = u"http://www.deq.state.or.us"

    start_urls = (u"http://www.deq.state.or.us/aqi",)

    def get_station_data(self, resp):
        raw_stations_data = resp.xpath(u"/html/head/script[last()]/text()").re(u"createMultiStation\((.*)\);")
        for station in raw_stations_data:
            # test_data = u"[{0}]".format(raw_stations_data[0])
            test_data = u"[{0}]".format(station)
            test_data = test_data.replace(u'"', u'\\"')
            test_data = test_data.replace(u"'", u'"')
            test_data = test_data.replace(u"role", u'"role"')
            # print(test_data)
            json = ujson.loads(test_data)

            raw_data = json[3][0]

            station_id = raw_data[0]
            # print(station_id)
            row_data_time = Selector(text=raw_data[len(raw_data) - 2])
            data_time = row_data_time.xpath(u"//td[2]/text()").extract_first()
            data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

            pollutant_data = raw_data[len(raw_data) - 4] + raw_data[len(raw_data) - 3]
            pollutant_data = Selector(text=pollutant_data)

            pollutants_name_p1 = pollutant_data.xpath(u"//table/tr[1]/td")[1:3]
            pollutants_name_p1 = [el.xpath(u"u/text()").extract_first() for el in pollutants_name_p1]
            pollutants_name_p2 = pollutant_data.xpath(u"//table/tr[1]/td")[1:3]
            pollutants_name_p2 = [el.xpath(u"sub/text()").extract_first() for el in pollutants_name_p2]

            pollutants_name = [u" ".join(x) for x in zip(pollutants_name_p1, pollutants_name_p2)]

            pollutant_value_data = pollutant_data.xpath(u"//table/tr[2]/td")[1:3]
            pollutant_value_data = [val.xpath(u"text()").extract_first().split(u" ") for val in pollutant_value_data]
            pollutant_value = [el[0] for el in pollutant_value_data]
            pollutant_units = [el[1] for el in pollutant_value_data]

            data = zip(pollutants_name, pollutant_value, pollutant_units)
            # print(data)

            station_data = dict()
            for record in data:
                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                # print("record", record)
                pollutant.set_raw_name(record[0])
                pollutant.set_raw_value(record[1])
                pollutant.set_raw_units(record[2])
                # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    station_data[pollutant.get_name()] = pollutant.get_value()

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        # self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
