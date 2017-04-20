# coding: utf-8
from scrapy import Spider
from re import findall as re_findall
from pytz import timezone
from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from dateutil.parser import parse
from datetime import datetime


class RbcaaSpider(Spider):
    name = u"za_rbcaa"
    tz = u"Etc/GMT+2"
    source = u"http://www.rbcaa.org.za"
    start_urls = (
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=1",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=2",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=3",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=4",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=5",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=6",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=9",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=11",
        u"http://live.rbcaa.org.za/StationInfo.aspx?ST_ID=16",
    )

    def check_datetime(self, row_datetime):
        data_time = parse(row_datetime)
        data_time = data_time.replace(tzinfo=timezone(self.tz))
        return data_time

    def get_station_data(self, resp):
        # station id
        regex = u"ST_ID=(.+)"
        station_id = re_findall(regex, resp.url)
        station_id = station_id[0]
        # print(station_id)

        # pollutions names
        row_names = resp.xpath(u'//*[@id="C1WebGrid1_grid01"]/tbody/tr[1]/td/div/text()').extract()
        row_names = row_names[1:]
        # print(row_names)
        names = list()
        for name in row_names:
            name = str(name).replace(u"\n", u"")
            name = str(name).replace(u"\t", u"")
            names.append(name)
            # print(name)

        data_table = resp.xpath(u'//*[@id="C1WebGrid1_grid11"]/tbody/tr[1]')

        # datetime
        row_data_time = data_table.xpath(u"td[1]/div/text()").extract_first()
        data_time = self.check_datetime(row_data_time)
        # print(datetime)

        # pollutions values
        row_values = data_table.xpath(u"td/div/text()").extract()
        row_values = row_values[1:]
        # print(row_values)
        values = list()
        for value in row_values:
            value = value.replace(u"\n", u"")
            value = value.replace(u"\t", u"")
            value = value.replace(u"\xa0", u"")
            if value == u"":
                value = None

            values.append(value)
            # print(value)

        # zip names and values
        data = zip(names, values)
        # print(data)

        station_data = dict()
        for val in data:
            _name = val[0]
            _val = val[1]
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
        for station in self.get_station_data(response):
            yield station
