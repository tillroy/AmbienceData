# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider, Request


class TasmaniaSpider(Spider):
    """дані оновлюються орієнтовно кожні 10 хвилин"""
    name = u"au_tasmania"
    tz = u"Australia/Tasmania"
    source = u"http://epa.tas.gov.au"
    station_data = (u"http://epa.tas.gov.au/air/live/epa_tas_latest_met.txt",)
    start_urls = station_data

    def get_date(self, date_str):
        row_date = date_str.lstrip(u"# File production initiated at YYYYMMDD:")
        row_date = row_date.replace(u", HHMMSS: ", u" ")
        date = parse(row_date).replace(tzinfo=timezone(self.tz))

        return date

    def get_station_data(self, resp):
        body = resp.body
        body = body.split(u"\r\n")
        col_names = body[8].lstrip(u"#")
        col_names = col_names.split(u", ")
        col_names = col_names[1:]
        # print(col_names)

        table = body[9:len(body)-1]
        weather_data = dict()
        for row in table:
            col = row.split(u",")
            col_values = list()

            for el in col:
                if u" " in el:
                    el = el.replace(u" ", u"")
                if u"/" in el:
                    el = None
                if u"-99" == el:
                    el = None
                if u"-999" == el:
                    el = None
                if u"-9999" == el:
                    el = None
                col_values.append(el)
            station_id = col_values[0]

            col_values = col_values[1:]
            # print(col_values)
            data = zip(col_names, col_values)

            station_data = dict()
            for st in data:
                _name = st[0]
                _val = st[1]
                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            weather_data[station_id] = station_data
            # print(station_data)

        # print(weather_data)
        return Request(
            u"http://epa.tas.gov.au/air/live/epa_tas_latest_particle_data.txt",
            callback=self.get_additional_data,
            meta=dict(data=weather_data)
        )

    def get_additional_data(self, resp):
        weather_data = resp.meta[u"data"]
        body = resp.body
        body = body.split(u"\r\n")
        col_names = body[8].lstrip(u"#")
        col_names = col_names.split(u", ")
        col_names = col_names[1:]
        # print(col_names)

        data_time = self.get_date(body[1])

        table = body[9:len(body) - 1]
        for row in table:
            col = row.split(u",")

            col_values = list()
            for el in col:
                if u" " in el:
                    el = el.replace(u" ", u"")
                if u"/" in el:
                    el = None
                if u"-99" == el:
                    el = None
                if u"-999" == el:
                    el = None
                if u"-9999" == el:
                    el = None
                col_values.append(el)

            station_id = col_values[0]
            col_values = col_values[1:]
            # print(col_values, station_id)

            data = zip(col_names, col_values)

            station_data = dict()
            for st in data:
                _name = st[0]
                _val = st[1]
                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            # доклеюємо дані із першої відповіді
            station_data.update(weather_data[station_id])

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        return self.get_station_data(response)
