# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class WashingtonSpider(Spider):
    name = u"us_washington"
    source = u"https://fortress.wa.gov"
    tz = u"US/Pacific"

    # get station info
    def start_requests(self):
        codes = (u"151", u"152", u"155", u"71", u"156", u"190", u"132", u"116", u"137", u"65", u"68", u"173", u"118",
                 u"24", u"25", u"123", u"27", u"20", u"21", u"46", u"119", u"42", u"29", u"40", u"41", u"1", u"2", u"5",
                 u"4", u"7", u"9", u"188", u"189", u"56", u"143", u"141", u"120", u"76", u"108", u"74", u"168", u"72",
                 u"126", u"127", u"164", u"166", u"106", u"163", u"10", u"13", u"38", u"15", u"58", u"17", u"16", u"32",
                 u"31", u"30", u"37", u"36", u"53", u"77", u"184", u"54", u"64", u"57")
        # codes = (u"151",)

        # for location
        # href = u"https://fortress.wa.gov/ecy/enviwa/StationDetails.aspx?"
        href = u"https://fortress.wa.gov/ecy/enviwa/StationInfo.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(href, u"ST_ID", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )


    # get codes_id
    def start_requests_id(self):
        codes_id = (u"22", u"23", u"64", u"75", u"80", u"81", u"90", u"109", u"110", u"118")
        # codes_id = (u"22",)

        href = u"https://fortress.wa.gov/ecy/enviwa/DynamicTable.aspx?"
        for code_id_value in codes_id:
            url = add_or_replace_parameter(href, u"G_ID", code_id_value)

            yield Request(
                url=url,
                callback=self.parse
            )

    def get_station_location(self, resp):
        raw_data = resp.xpath(u'//*[@id="form1"]/div[3]/table/tr[2]/td[1]/table/tr')
        station_name = raw_data[0].xpath(u"td[2]/span/text()").extract_first()
        station_address = raw_data[1].xpath(u"td[2]/span/text()").extract_first()

        lon = raw_data[5].xpath(u"td[2]/span/text()").extract_first()
        lat = raw_data[6].xpath(u"td[2]/span/text()").extract_first()
        elev = raw_data[7].xpath(u"td[2]/span/text()").extract_first()

        res = ";".join((resp.meta[u"code"], station_name, station_address, lon, lat, elev, u"\n"))
        open(u"us_washington_stations.txt", "a").write(res)
        # print(resp.meta[u"code"], station_name, station_address, lon, lat, elev)

    def get_station_id(self, resp):
        raw_ids = resp.xpath(u'//*[@id="C1WebGrid1"]/tr')[2:]
        raw_ids = [el.xpath(u"td[1]/div/a/@href").re(u"ST_ID=(.+)")[0] for el in raw_ids]

        yield raw_ids

    def get_station_data(self, resp):
        data_time = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[3]/td[1]/div/text()').re(u"\r\n\t(.+)\r\n")[0]
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

        pollutant_name = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[1]/td')[1:]
        pollutant_name = [el.xpath(u"div/text()").re(u"\r\n\t(.+)\r\n")[0] for el in pollutant_name]
        # print(pollutant_name)

        pollutant_units = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[2]/td')[1:]
        pollutant_units = [el.xpath(u"div/text()").re(u"\r\n\t(.+)\r\n")[0] for el in pollutant_units]
        # print(pollutant_units)

        pollutant_value = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[3]/td')[1:]
        pollutant_value = [el.xpath(u"div/text()").re(u"\r\n\t(.+)\r\n")[0] for el in pollutant_value]

        data = zip(pollutant_name, pollutant_value, pollutant_units)

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
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        # self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el
