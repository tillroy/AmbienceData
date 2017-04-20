# coding: utf-8
from scrapy import Spider, Request
from ujson import loads
from pytz import timezone
from dateutil.parser import parse
from pollution_app.settings import SCRAPER_TIMEZONE
from pollution_app.items import AppItem
from pollution_app.pollution import Kind
from datetime import datetime


class SincaSpider(Spider):
    name = u"cl_sinca"
    tz = u"Chile/Continental"
    source = u"http://sinca.mma.gob.cl"

    # start_urls = ("http://sinca.mma.gob.cl/index.php/json/listado/",)
    def start_requests(self):
        ids1 = (u"D28", u"D30", )
        ids = (
            u"117", u"201", u"204", u"222", u"228", u"230", u"231", u"233", u"235", u"236", u"237", u"332", u"333",
            u"414", u"420", u"424", u"425", u"426", u"501", u"503", u"504", u"505", u"506", u"507", u"509", u"511",
            u"512", u"521", u"522", u"523", u"524", u"529", u"535", u"540", u"547", u"548", u"549", u"550", u"609",
            u"611", u"612", u"615", u"703", u"709", u"710", u"711", u"713", u"802", u"804", u"806", u"807", u"809",
            u"810", u"815", u"816", u"827", u"831", u"837", u"838", u"846", u"850", u"854", u"859", u"860", u"861",
            u"871", u"873", u"875", u"876", u"877", u"901", u"902", u"904", u"A01", u"A06", u"A07", u"B03", u"B04",
            u"C05", u"D11", u"D12", u"D13", u"D14", u"D15", u"D16", u"D17", u"D18", u"D27", u"D28", u"D30", u"E03",
            u"F01"
        )

        for station_id in ids:
            url = u"http://sinca.mma.gob.cl/cgi-bin/stnresume.cgi?domain=CONAMA&stn={0}".format(station_id)
            yield Request(
                url=url,
                meta={u"station_id": station_id},
                callback=self.parse
            )

    def get_datetime(self, resp):
        _datetime = resp.xpath(u"/html/body/table/tbody/tr[1]/td[2]/div/text()").extract_first()
        print(_datetime)
        data_time = parse(_datetime)
        data_time = data_time.replace(tzinfo=timezone(self.tz))

        return data_time

    # TODO maybe move to a separated file
    @staticmethod
    def remove_apostrophe(string):
        if u"'" in string:
            string = string.replace(u"'", u"''")
            print(string)

        string = string.replace(u"\n", u"")

        return string

    def parse_json(self, resp):
        json = loads(resp.body)

        # open("../SA/cl_mma/cl_mma_stations.csv", "w").write(", ".join(("station_name", "lon", "lat", "id", "\n")))
        open(u"../sql/db/cl_mma_add_station.sql", "w").close()

        for record in json:
            station_name = record[u"nombre"]
            if station_name is None:
                station_name = u"NA"

            station_name = self.remove_apostrophe(station_name.encode(u"utf-8"))
            lat = str(record[u"latitud"])
            lon = str(record[u"longitud"])
            address = self.remove_apostrophe(record[u"region"].encode(u"utf-8"))
            station_code = self.remove_apostrophe(record[u"key"].encode(u"utf-8"))

            open('../sql/db/cl_mma_add_station.sql', 'a').write('''
            INSERT INTO station(lon, lat, address, source, code, name)
            VALUES({0}, {1}, {2}, {3}, {4}, {5});
            '''.format(
                lon,
                lat,
                "'" + str(address) + "'",
                "'http://sinca.mma.gob.cl'",
                "'" + str(station_code) + "'",
                "'" + str(station_name) + "'")
            )

    def get_station_data(self, resp):
        table = resp.xpath(u"/html/body/table/tbody/tr")
        data_time = self.get_datetime(resp)

        station_data = dict()
        for record in table:
            pollution_name = record.xpath(u"td[3]/text()").extract_first()
            pollution_value = record.xpath(u"td[4]/text()").extract_first()
            if len(pollution_value.split(u" ")) > 1:
                try:
                    pollution_value = pollution_value.split(u" ")[0]
                except IndexError:
                    pollution_value = None

            _dict = Kind(self.name).get_dict(r_key=pollution_name, r_val=pollution_value)
            if _dict:
                station_data[_dict[u"key"]] = _dict[u"val"]

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"station_id"]

            yield items

    def parse(self, response):
        # self.parse_json(response)
        for station in self.get_station_data(response):
            yield station
