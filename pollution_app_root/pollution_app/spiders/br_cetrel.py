# coding: utf-8
from scrapy import Spider
from dateutil.parser import parse
from pytz import timezone
from datetime import datetime
from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.rextension import clean
from pollution_app.settings import SCRAPER_TIMEZONE


class CetrelSpider(Spider):
    name = u"br_cetrel"
    tz = u"Brazil/East"
    source = u"http://www.odebrechtambiental.com"
    start_urls = (u"http://qualidadear.cetrel.com.br/iframe/",)

    def check_data_time(self, row_data_time):
        row_data_time = parse(clean(row_data_time))
        data_time = row_data_time.replace(tzinfo=timezone(self.tz))
        return data_time

    def get_station_data(self, resp):
        tabs = resp.xpath(u'//*[@id="Form1"]/div[3]/div[2]/article')[1:]
        for tab in tabs:
            row_data_time = tab.xpath(u"header/span/text()").extract_first()
            data_time = self.check_data_time(row_data_time)

            row_data_table = tab.xpath(u"section/ul/li")
            for row in row_data_table:
                station_name = row.xpath(u"a/h3/text()").extract_first()
                station_id = station_name
                pollution_table = row.xpath(u"div/div[3]/table/tbody/tr")

                station_data = dict()
                for pollution in pollution_table:
                    # pollution names
                    pollution_name = pollution.xpath(u"td[1]/text()").extract_first()

                    sub_pollution_name = pollution.xpath(u"td[1]/sub/text()").extract_first()
                    if sub_pollution_name is not None:
                        pollution_name += sub_pollution_name

                    pollution_name = pollution_name.split(u" \u2013 ")[1]
                    pollution_name = clean(pollution_name)
                    # print(repr(pollution_name))
                    # pollution values
                    pollution_value = pollution.xpath(u"td[2]/text()").extract_first()
                    pollution_value = clean(pollution_value, (u"\xcdndice:",))

                    # print(repr(pollution_value))

                    _tmp_dict = Kind(self.name).get_dict(r_key=pollution_name, r_val=pollution_value)
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
