# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class NewSouthWalesSpider(Spider):
    name = u"au_new_south_wales"
    tz = u"Australia/NSW"
    source = u"http://www.environment.nsw.gov.au"
    station_data = (u"http://airquality.environment.nsw.gov.au/aquisnetnswphp/getPage.php?reportid=2",)
    start_urls = station_data

    def get_date(self, resp):
        row_date = resp.xpath(u"/html/body/table[1]/tbody/tr/td[1]/text()").extract()
        row_date = row_date[:len(row_date) - 4]

        date = u" ".join(row_date)
        if u"pm" in date:
            date = date.split(u" - ")
            date = date[0]
            date += u" pm"
        elif u"am" in date:
            date = date.split(u" - ")
            date = date[0]
            date += u" am"

        date = parse(date).replace(tzinfo=timezone(self.tz))

        return date

    def get_station_data(self, resp):
        data_time = self.get_date(resp)
        print(data_time)

        row_col_names = resp.xpath(u"/html/body/table[2]/tbody/tr[1]/th/a")
        col_names = list()
        for col_name in row_col_names:
            name = col_name.xpath(u"text()").extract()
            if len(name) > 1:
                name = u" ".join(name)
            elif len(name) == 1:
                name = name[0]
            else:
                name = None
            col_names.append(name)

        # print(col_names)

        row_col_types = resp.xpath(u"/html/body/table[2]/tbody/tr[2]/th")
        col_types = list()
        for col_type in row_col_types:
            name = col_type.xpath(u"text()").extract()
            if len(name) > 1:
                name = u" ".join(name)
            elif len(name) == 1:
                name = name[0]
            else:
                name = None

            name = name.replace(u'\xa0', u"")
            col_types.append(name)

        col_types = col_types[1:]
        # print(col_types)

        full_col_names = map(lambda x, y: u" ".join((x, y)), col_names, col_types)
        full_col_names[len(full_col_names)-2] += u" (PM10)"
        full_col_names[len(full_col_names)-1] += u" (PM25)"
        # print(full_col_names)

        # опрацювання значень таблиці
        row_table = resp.xpath(u"/html/body/table[2]/tbody/tr")
        row_table = row_table[3:len(row_table)-2]

        for row in row_table:
            col = row.xpath(u"td")
            data_values = list()
            for row_value in col:
                value = row_value.xpath(u"text()").extract()
                try:
                    data_values.append(value[0])
                except IndexError:
                    data_values.append(None)

            if len(data_values) == 11:
                data_values.insert(0, None)
                data_values = data_values[:len(data_values)-2]
            if len(data_values) == 14:
                data_values = data_values[:len(data_values)-4]

            station_id = data_values[1]
            data_values = data_values[2:]
            # print(len(data_values), data_values, station_id)

            data = zip(full_col_names, data_values)

            station_data = dict()
            for st in data:
                _name = st[0]
                _val = st[1]
                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        # self.get_station_data(response)
        for st in self.get_station_data(response):
            yield st
