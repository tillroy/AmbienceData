# coding: utf-8
from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter

from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class VirginiaSpider(Spider):
    name = u"us_virginia"
    source = u"http://vadeq.tx.sutron.com"
    tz = u"US/Eastern"

    def start_requests(self):
        codes = (u"28", u"10", u"17", u"25", u"26", u"24", u"13", u"14", u"18", u"19", u"37", u"30", u"31", u"34",
                 u"35", u"11", u"12", u"15", u"16", u"20", u"21", u"23", u"27", u"39")
        # codes = (u"11",)

        url = u"http://vadeq.tx.sutron.com/cgi-bin/daily_summary.pl?"

        for code_value in codes:
            url = add_or_replace_parameter(url, u"cams", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    @staticmethod
    def validate_hours(hours):
        mid_ind = hours.index(u"Mid")

        try:
            noon_ind = hours.index(u"Noon")
        except ValueError:
            noon_ind = None

        if noon_ind is None:
            hours[mid_ind] = u"0:00"
            hours = [x.strip(u" ") for x in hours]
            return hours

        elif noon_ind is not None:
            hours[mid_ind] = u"0"
            hours[noon_ind] = u"12"
            hours = [int(x.strip(u" ").replace(u":00", u"")) for x in hours]

            new_hours = list()
            for key, el in enumerate(hours):
                if key > noon_ind:
                    new_hours.append(el + 12)
                else:
                    new_hours.append(el)

            hours = [u"{0}:00".format(x) for x in new_hours]
            return hours

    def get_station_data(self, resp):
        table = resp.xpath(u'//*[@id="meteostar_wrapper"]/div[2]/table/tr')[2:-4]
        pollutants_hour = resp.xpath(u'//*[@id="meteostar_wrapper"]/div[2]/table/tr[2]/th/text()').extract()
        pollutants_hour = self.validate_hours(pollutants_hour)
        hour = pollutants_hour[len(pollutants_hour) - 2] if len(pollutants_hour) > 1 else pollutants_hour[0]

        data_time = resp.xpath(u'//*[@id="meteostar_wrapper"]/p[5]/b/text()').extract_first()
        data_time = u" ".join((data_time, hour))
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

        units = {
            u"o3": u"ppb",
            u"pm25": u"ug/m3",
            u"co": u"ppm",
            u"so2": u"ppb",
            u"no2": u"ppb",
            u"no": u"ppb",
            u"n2o": u"ppb",
            u"ws": u"mph",
            u"wd": u"deg",
            u"temp": u"degf",
            u"pres": u"mbar",
        }

        station_data = dict()
        for row in table:
            pollutant_name = row.xpath(u"td[1]/a/b/text()").extract_first()
            # print(pollutant_name)
            pollutants_data = row.xpath(u"td[last()-3]").re(u">(\d+?)<|>(\d+?\.\d+)<")
            pollutant_value = [el for el in pollutants_data if el != u""]
            pollutant_value = pollutant_value[0] if pollutant_value else None

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            # print("record", record)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            try:
                pollutant.set_raw_units(units[pollutant.get_name()])
            except KeyError:
                print("There is no such pollutant in local units list <<<<<<<<{0}>>>>>>".format(pollutant.get_name()))

            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
