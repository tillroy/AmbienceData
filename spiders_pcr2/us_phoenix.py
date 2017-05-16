# coding: utf-8

from datetime import datetime

from scrapy import Spider
from pytz import timezone
from dateutil import parser

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class PhoenixSpider(Spider):
    name = u"us_phoenix"
    source = u"http://www.fcd.maricopa.gov/Default.aspx"
    tz = u"US/Pacific"
    start_urls = (u"http://alert.fcd.maricopa.gov/alert/Google/xml/fcdmc_alert_aqwx.xml",)

    def get_station_data(self, resp):
        data = resp.xpath(u"//FCDMC/gage_wx")
        for record in data:
            data_time = record.xpath(u"@AT_date_time").extract_first()
            data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))

            station_id = record.xpath(u"@name").extract_first()
            data = {
                u"co": record.xpath(u"@CO").extract_first(),
                u"no2": record.xpath(u"@NO2").extract_first(),
                u"o3": record.xpath(u"@O3").extract_first(),
                u"pm10": record.xpath(u"@PM10").extract_first(),
                u"pm25": record.xpath(u"@PM25").extract_first(),
                u"bp": record.xpath(u"@BP").extract_first(),
                u"rhum": record.xpath(u"@RHUM").extract_first(),
                u"so2": record.xpath(u"@SO2").extract_first(),
                u"AMBIENTTEMP": record.xpath(u"@AT").extract_first(),
                u"wd": record.xpath(u"@WD").extract_first(),
                u"ws": record.xpath(u"@WS").extract_first(),
                u"rain": record.xpath(u"@RAIN").extract_first(),
                u"dt": record.xpath(u"@DT").extract_first(),
                u"solar": record.xpath(u"@SOLAR").extract_first(),
            }

            station_data = dict()
            for name, value in data.items():
                pollutant = Kind(self.name).get_dict(r_key=name, r_val=value)
                if pollutant:
                    station_data[pollutant[u"key"]] = pollutant[u"val"]

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
