# coding: utf-8
from scrapy import Spider
from dateutil.parser import parse
from pytz import timezone
from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from datetime import datetime
from pollution_app.settings import SCRAPER_TIMEZONE


class RioSpider(Spider):
    name = u"br_rio"
    tz = u"Brazil/East"
    source = u"http://www.rio.rj.gov.br"
    start_urls = (u"http://smac.infoper.net/smac/boletim",)

    def check_data_time(self, row_data_time):
        row_data_time = parse(row_data_time.rstrip(u"h"))
        data_time = row_data_time.replace(tzinfo=timezone(self.tz))
        return data_time

    def get_station_data(self, resp):
        # get data time
        row_data_time = resp.xpath(u'//*[@id="titulo"]').re(u"<h4>(.+)</h4>")[0]
        data_time = self.check_data_time(row_data_time)

        # get data
        row_data_table = resp.xpath(u'//*[@id="dados_estacoes"]/table/tr')[2:]

        for row in row_data_table:
            colspan_check = row.xpath(u"td/@colspan").extract()
            if not colspan_check:
                cols = row.xpath(u"td/text()").extract()
                station_name = cols[0]
                station_id = station_name
                print(cols)
                so2 = (u"so2", cols[1].strip(u" "))
                co = (u"co", cols[2].strip(u" "))
                pm10 = (u"pm10", cols[3].strip(u" "))
                o3 = (u"o3", cols[4].strip(u" "))
                no2 = (u"no2", cols[5].strip(u" "))

                # full pollution data
                data = (so2, co, pm10, o3, no2)

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
