# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.aqi import Aqi
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class WaSpider(Spider):
    name = u"au_wa"
    start_urls = (u"https://www.der.wa.gov.au/your-environment/air/air-quality-index",)
    tz = u"Australia/West"
    source = u"https://www.der.wa.gov.au"


    def get_st_data(self, resp):
        date = resp.xpath(u'//*[@id="MainContent"]/div[2]/div[2]/ul/li/span/a/text()').extract_first()
        date = date.replace(u"\t", u"")
        date = date.replace(u"\nAir Quality Index ", u"")

        all_tables = resp.xpath(u'//*[@class="table table-alternate table-condensed"]')
        tables = [x for i, x in enumerate(all_tables) if i % 2 == 0]
        tables_date = [x for i, x in enumerate(all_tables) if i % 2 != 0]
        tables = tables[:len(tables) - 1]
        for index, table in enumerate(tables):
            hour = tables_date[index].xpath(u"tbody/tr/td/text()").re_first(u"AQI at (\d+) hrs")
            row_data_time = date + u" " + hour
            data_time = parse(row_data_time).replace(tzinfo=timezone(self.tz))

            station_id = table.xpath(u"thead/tr/th[1]/text()").re_first(u"(.+) A.Q.M.S.")
            if u" Particles" in station_id:
                station_id = station_id.replace(u" Particles", u"")
            if u" Mobile" in station_id:
                station_id = station_id.replace(u" Mobile", u"")

            rows = table.xpath(u"tbody/tr")

            station_data = dict()
            for row in rows:
                name = row.xpath(u"td[1]/text()").extract_first()
                name = name.replace(u" ", u"")
                aqi = float(row.xpath(u"td[3]/text()").extract_first())

                if name == u"SO2":
                    val_name = u"so2"
                elif name == u"NO2":
                    val_name = u"no2"
                elif name == u"O3":
                    val_name = u"o3"
                elif name == u"PM10":
                    val_name = u"pm10"
                elif name == u"PM2.5":
                    val_name = u"pm25"
                elif name == u"CO":
                    val_name = u"co"
                else:
                    val_name = None

                if val_name:
                    val = Aqi().aqi_to_val(aqi, val_name)

                    _tmp_dict = Kind(self.name).get_dict(r_key=name, r_val=val)
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
        # self.get_st_data(response)
        for st in self.get_st_data(response):
            yield st
