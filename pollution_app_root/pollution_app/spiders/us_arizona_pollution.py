# coding: utf-8

from datetime import datetime

from scrapy import Spider
from scrapy_splash import SplashRequest
from dateutil import parser
from pytz import timezone
import ujson

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ArizonaSpider(Spider):
    name = u"us_arizona_pollution"
    source = u"http://www.azdeq.gov"
    tz = u"US/Pacific"

    def start_requests(self):
        yield SplashRequest(
            url=u"http://airdata.azdeq.gov/AirVision/Custom/GoogleMap.aspx/GetSitesInfo",
            args={
                # u"wait": 10,
                u"http_method": u"POST",
                u"headers": {
                    u"Content-Type": u"application/json",
                }
            },
            # callback=self.parse,
            callback=self.get_station_data,
        )

    def get_station_data(self, resp):
        data = resp.xpath(u"//pre/text()").extract_first()
        json = ujson.loads(data)
        stations = json[u"d"]
        for st in stations:
            data_time = st[u"AqiTimeString"]
            data_time = parser.parse(data_time)

            station_id = st[u"SourceSiteID"]

            station_data = dict()
            for record in st[u"LayerInfos"]:
                pollutant_name = record.get(u"ParameterName")
                pollutant_unit = record.get(u"UnitName")
                pollutant_value = record.get(u"Concentration")

                # print(pollutant_name, pollutant_value, pollutant_unit)

                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                pollutant.set_raw_name(pollutant_name)
                pollutant.set_raw_value(pollutant_value)
                pollutant.set_raw_units(pollutant_unit)

                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    station_data[pollutant.get_name()] = pollutant.get_value()

            # print(station_data)

            data_time = data_time.replace(tzinfo=timezone(self.tz))

            if station_data and data_time:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items
