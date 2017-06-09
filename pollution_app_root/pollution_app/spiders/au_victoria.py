# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone
from ujson import loads as js_loads

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class VictoriaSpider(Spider):
    name = u"au_victoria"
    tz = u"Australia/Victoria"
    source = u"http://www.epa.vic.gov.au"
    st_data = (u"http://www.epa.vic.gov.au/services/MiddleTierDataService.svc/getairqualityairdatalist",)
    start_urls = st_data

    @staticmethod
    def get_st_pos(resp):
        row_json = resp.body
        row_json = row_json.replace(u"\xef\xbb\xbf", u"")
        json = js_loads(row_json)

        stations = json[u"Stations"]

        file_name = u"add_map_au_victoria.sql"
        open(file_name, u"w").close()

        for st in stations:
            st_name = st[u"Station"]
            region = st[u"Region"]
            addr = region + u", " + st_name
            lat = st[u"Latitude"]
            lon = st[u"Longitude"]
            st_id = st_name
            url = u"http://www.epa.vic.gov.au"

            query = """
                INSERT INTO scrapper_map(lon, lat, address, source_id, source, st_name)
                VALUES({0},{1},{2},{3},{4},{5});
            """

            res = query.format(
                str(lon),
                str(lat),

                """ + str(addr) + """,
                """ + str(st_id) + """,
                """ + str(url) + """,
                """ + str(st_name) + """
            )
            open(file_name, "a").write(res)

    def get_date(self, row_date):
        row_date = row_date.split(u" - ")
        row_date = row_date[1]
        data_time = parse(row_date).replace(tzinfo=timezone(self.tz))

        return data_time

    def get_st_data(self, resp):
        row_json = resp.body.decode(u"utf-8")
        row_json = row_json.replace(u"\ufeff", u"")
        json = js_loads(row_json)

        data_time = self.get_date(json[u"DateTime"])
        stations = json[u"Stations"]

        for st in stations:
            name = st[u"Station"]
            params = st[u"ParameterValueList"]

            st_data = dict()
            for p in params:
                pol_name = p[u"Id"]
                pol_value = p[u"Value"]

                _tmp_dict = Kind(self.name).get_dict(r_key=pol_name, r_val=pol_value)
                if _tmp_dict:
                    st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = name

                yield items

    def parse(self, response):
        for st in self.get_st_data(response):
            yield st
