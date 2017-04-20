# coding: utf-8
from scrapy import Spider
import ujson
import pandas as pd

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from dateutil.parser import parse
from pytz import timezone
from pollution_app.settings import SCRAPER_TIMEZONE
from datetime import datetime


class USembassySpider(Spider):
    name = u"all_usembassy"
    source = u"https://www.airnow.gov"
    # start_urls = (u"http://newdelhi.usembassy.gov/airqualitydataemb.html",)
    # AIRNOW department https://www.airnow.gov/index.cfm?action=airnow.global_summary#India$New_Delhi
    # start_urls = (u"http://stateair.net/dos/RSS/NewDelhi/NewDelhi-PM2.5.xml",)
    start_urls = (u"http://stateair.net/dos/AllPosts24Hour.json",)
    tzs = {
        u"Lima": u"America/Lima",
        u"Bogota": u"America/Bogota",
        u"Dhaka": u"Asia/Dhaka",
        u"Kolkata": u"Asia/Kolkata",
        u"New Delhi": u"Asia/Calcutta",
        u"Mumbai": u"Asia/Calcutta",
        u"Pristina": u"Europe/Belgrade",
        u"Hanoi": u"Etc/GMT+7",
        u"Addis Ababa School": u"Etc/GMT+3",
        u"Addis Ababa Central": u"Etc/GMT+3",
        u"Chennai": u"Asia/Calcutta",
        u"Ho Chi Minh City": u"Etc/GMT+7",
        u"Hyderabad": u"Asia/Calcutta",
        u"Jakarta Central": u"Etc/GMT+7",
        u"Ulaanbaatar": u"Asia/Ulaanbaatar",
        u"Jakarta South": u"Etc/GMT+7"
    }

    def get_station_position(self, resp):
        json = ujson.loads(resp.body)
        station_position = list()
        for key, obj in json.items():
            coord = obj[u"coordinates"]
            res = {
                u"station_name": key,
                u"lat": coord[0],
                u"lon": coord[1],
                u"address": u"-",
                u"station_id": key,
            }
            station_position.append(res)

        df = pd.DataFrame(station_position)
        df.to_csv(u"all_usembassy.csv", sep="|", index=False)

    def get_timezone(self, location):
        return self.tzs.get(location)

    def get_station_data(self, resp):
        json = ujson.loads(resp.body)

        station_position = list()
        for station_name, obj in json.items():
            pollutant_name = obj[u"monitors"][0][u"parameter"]
            local_time = obj[u"monitors"][0][u"beginTimeLT"]
            pollutant_value = obj[u"monitors"][0][u"conc"][:1][0]

            # res = {
            #     u"station_id": station_name,
            #     u"pollutant_name": pollutant_name,
            #     u"local_time": local_time,
            #     u"pollutant_value": pollutant_value
            # }

            data_time = parse(local_time)
            data_time = data_time.replace(tzinfo=timezone(self.get_timezone(station_name)))

            station_data = dict()
            _tmp_dict = Kind(self.name).get_dict(r_key=pollutant_name, r_val=pollutant_value)
            if _tmp_dict:
                station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_name

                yield items

    def parse(self, response):
        for station in self.get_station_data(response):
            yield station

