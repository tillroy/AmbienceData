# coding: utf-8
from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from pytz import timezone
from dateutil import parser

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class IndinaSpider(Spider):
    name = u"us_indiana_pollution"
    source = u"http://idem.tx.sutron.com/"
    tz = u"US/Eastern"

    def start_requests(self):
        codes = (u"6", u"11", u"13", u"16", u"28", u"30", u"31", u"34", u"47", u"82", u"5", u"18",
                 u"21", u"72", u"74", u"7", u"19", u"27", u"70", u"9", u"36", u"37", u"38", u"51",
                 u"1", u"25", u"35", u"39", u"40", u"42", u"55", u"56", u"57", u"58", u"60", u"63",
                 u"64", u"65", u"66", u"68", u"69", u"71", u"73", u"76", u"77", u"78", u"79", u"80",
                 u"81", u"12", u"15", u"20", u"24", u"33", u"46", u"49", u"52", u"53", u"67", u"3",
                 u"4", u"59")
        # codes = (u"9",)

        url = u"http://idem.tx.sutron.com/cgi-bin/daily_summary.pl?"

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
        table = resp.xpath(u'//*[@id="meteostar_wrapper"]/div[3]/table/tr')[2:-4]

        pollutants_hour = resp.xpath(u'//*[@id="meteostar_wrapper"]//table[1]/tr[2]/th/text()').extract()

        pollutants_hour = self.validate_hours(pollutants_hour)
        hour = pollutants_hour[len(pollutants_hour) - 2] if len(pollutants_hour) > 1 else pollutants_hour[0]

        data_time = resp.xpath(u'//*[@id="meteostar_wrapper"]/p[5]/b/text()').extract_first()
        data_time = u" ".join((data_time, hour))
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

        station_data = dict()
        for row in table:
            raw_pollutant_name = row.xpath(u"td[1]/a/b/text()")
            pollutant_name = raw_pollutant_name.extract()[0] if len(raw_pollutant_name) == 1 else None
            # print(pollutant_name)

            raw_pollutant_unit = row.xpath(u"td[1]/a/@onmouseover")

            # print(raw_pollutant_unit.extract())

            pollutant_unit = raw_pollutant_unit.re(u"\d*\.\s+(\D+)',")
            pollutant_unit = pollutant_unit[0].replace("Measured in ", "") if len(pollutant_unit) == 1 else None

            pollutants_data = row.xpath(u"td[last()-4]").re(u">(\d+?)<|>(\d+?\.\d+)<")
            # print(pollutants_data)
            pollutant_value = [el for el in pollutants_data if el != u""]
            pollutant_value = pollutant_value[0] if pollutant_value else None

            # print(pollutant_name, pollutant_value)

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            # print("record", record)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            pollutant.set_raw_units(pollutant_unit)

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
        # self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
