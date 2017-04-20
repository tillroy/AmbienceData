# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class NewYorkSpider(Spider):
    name = u"us_new_york"
    source = u"http://www.nyaqinow.net"
    tz = u"US/Eastern"
    custom_settings = {
        u"DOWNLOAD_DELAY": 2,
    }
    # ids = set()

    def start_requests(self):
        # for id
        # codes = (u"0", u"1", u"2", u"3", u"4", u"5", u"7", u"8", u"9", u"10", u"11")

        codes = (u"56", u"42", u"50", u"60", u"61", u"62", u"63", u"53", u"134", u"80", u"24", u"25", u"21", u"49",
                 u"46", u"44", u"45", u"28", u"40", u"1", u"3", u"5", u"6", u"8", u"13", u"77", u"75", u"73", u"106",
                 u"79", u"10", u"39", u"38", u"15", u"19", u"18", u"57", u"30", u"36", u"35", u"34", u"33")
        # codes = (u"40",)

        # for id
        # href = u"http://www.nyaqinow.net/DynamicTable.aspx?"
        # for location
        # href = u"http://www.nyaqinow.net/StationDetails.aspx?"
        # for datascrapy crawl
        href = u"http://www.nyaqinow.net/StationInfo.aspx?"
        for code_value in codes:
            # for id
            # url = add_or_replace_parameter(href, u"G_ID", code_value)
            # for location
            url = add_or_replace_parameter(href, u"ST_ID", code_value)
            # for data
            # url = add_or_replace_parameter(href, u"ST_ID", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_id(self, resp):
        raw_ids = resp.xpath(u'//*[@id="C1WebGrid1"]/tr')[2:]
        # print(raw_ids)
        raw_ids = [el.xpath(u"td[1]/div//a/@href").re(u"ST_ID=(.+)")[0] for el in raw_ids]

        return raw_ids

    def get_station_location(self, resp):
        raw_data = resp.xpath(u'//*[@id="form1"]/div[3]/table/tr[2]/td[1]/table/tr')
        station_name = raw_data[0].xpath(u"td[2]/span/text()").extract_first()
        station_address = raw_data[1].xpath(u"td[2]/span/text()").extract_first(default=u"-")

        lon = raw_data[5].xpath(u"td[2]/span/text()").extract_first(default=u"")
        lat = raw_data[6].xpath(u"td[2]/span/text()").extract_first(default=u"")
        elev = raw_data[7].xpath(u"td[2]/span/text()").extract_first(default=u"")
        # print(resp.meta[u"code"], station_name, station_address, lon, lat, elev)
        res = u";".join((resp.meta[u"code"], station_name, station_address, lon, lat, elev, u"\n"))
        open(u"us_new_york_stations.txt", u"a").write(res)
        # print(resp.meta[u"code"], station_name, station_address, lon, lat, elev)

    def get_station_data(self, resp):
        raw_poll_name = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[1]/td')[1:]
        poll_name = [el.xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0] for el in raw_poll_name]

        raw_poll_units = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[2]/td')[1:]
        poll_units = [el.xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0] for el in raw_poll_units]

        raw_poll_value_data = resp.xpath(u'//*[@id="C1WebGrid1"]/tr[last()]/td')

        raw_poll_value = raw_poll_value_data[1:]
        poll_value = [el.xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0] for el in raw_poll_value]

        raw_data_time = raw_poll_value_data[0].xpath(u".//div[1]/text()").re(u"\r\n\t(.+)\r\n")[0]
        data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

        data = zip(poll_name, poll_value, poll_units)

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
        # self.get_station_location(response)
        for el in self.get_station_data(response):
            yield el