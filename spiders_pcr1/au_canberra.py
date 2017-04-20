# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone
from ujson import loads as js_parse

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class CanberraSpider(Spider):
    name = u"au_canberra"
    start_urls = (u"https://www.data.act.gov.au/resource/rfi5-rh86.json?$limit=3",)
    tz = u"Australia/Canberra"
    source = u"http://health.act.gov.au"

    @staticmethod
    def get_st_pos(resp):
        body = resp.body
        body = body.replace(u'\n', '')
        json = js_parse(body)

        file_name = 'add_map_au_canberra.sql'
        open(file_name, 'w').close()

        for st in json:
            st_name = st['name']
            lat = st['gps']['latitude']
            lon = st['gps']['longitude']
            addr = st_name
            st_id = st_name
            url = 'http://health.act.gov.au'

            query = """
                    INSERT INTO scrapper_map(lon, lat, address, source_id, source, st_name)
                    VALUES({0},{1},{2},{3},{4},{5});
                """

            res = query.format(
                str(lat),
                str(lon),
                "'" + str(addr) + "'",
                "'" + str(st_id) + "'",
                "'" + str(url) + "'",
                "'" + str(st_name) + "'"
            )
            open(file_name, 'a').write(res)

    def get_date(self, date):
        data_time = parse(date).replace(tzinfo=timezone(self.tz))

        return data_time

    def get_st_data(self, resp):
        body = resp.body
        body = body.replace(u"\n", u"")
        json = js_parse(body)
        exception = (u"name", u"gps", u"date", u"datetime", u"time")
        for station in json:
            station_name = station[u"name"]
            data_time = self.get_date(station[u"datetime"])

            station_data = dict()
            for attr_name in station:
                if attr_name not in exception:
                    pol_name = attr_name
                    pol_value = station[attr_name]

                    _tmp_dict = Kind(self.name).get_dict(r_key=pol_name, r_val=pol_value)
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
        for st in self.get_st_data(response):
            yield st
