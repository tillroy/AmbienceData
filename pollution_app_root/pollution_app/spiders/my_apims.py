# coding: utf-8
from datetime import datetime

from dateutil.parser import parse
from pytz import timezone

from pollution_app.aqi import Aqi
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from pollution_app.pollution import Kind

from scrapy import Spider, Request


class MalaysiaSpider(Spider):
    name = u"my_apims"
    tz = u"Asia/Kuala_Lumpur"
    source = u"http://apims.doe.gov.my"

    @staticmethod
    def get_quarter(time):
        if 0 <= time <= 5:
            return 1
        elif 6 <= time <= 11:
            return 2
        elif 12 <= time <= 17:
            return 3
        elif 18 <= time <= 23:
            return 4

    def start_requests(self):
        local_today = datetime.now(timezone(self.tz))
        quarter = self.get_quarter(local_today.hour)
        url = u"http://apims.doe.gov.my/v2/hour{quarter}_{year}-{month}-{day}.html".format(
            year=local_today.year,
            month=local_today.month,
            day=local_today.day,
            quarter=quarter
        )

        yield Request(
            url=url,
            callback=self.parse,
            meta={
                u"year": local_today.year,
                u"month": local_today.month,
                u"day": local_today.day,
                u"quarter": quarter,
            }
        )

    def get_date(self, resp):
        date = resp.xpath(u'//*[@id="topbar"]/div/ol/li[1]/a/text()').extract_first()
        row_date = date.lstrip(u"LATEST UPDATE : ")
        dt = parse(row_date)
        new_dt = dt.replace(tzinfo=timezone(self.tz))

        return new_dt

    def aqi_to_value(self, row_aqi):
        pollutant_name = None
        if row_aqi is not None:
            if u"*" in row_aqi:
                pollutant_name = u"pm10"
            elif u"a" in row_aqi:
                pollutant_name = u"so2"
            elif u"b" in row_aqi:
                pollutant_name = u"no2"
            elif u"c" in row_aqi:
                pollutant_name = u"o3"
            elif u"d" in row_aqi:
                pollutant_name = u"co"

        station_data = dict()
        if pollutant_name is not None:
            val = row_aqi[:-1]
            # конвертуєму в з начення якщо це можливо
            val = Aqi().aqi_to_val(val, pollutant_name)

            _tmp_dict = Kind(self.name).get_dict(r_key=pollutant_name, r_val=val)
            # print("res", _tmp_dict)
            if _tmp_dict:
                station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        # print station_data
        return station_data

    def get_station_data(self, resp):
        time_header = resp.xpath(u'//*[@id="content"]/div/table/tr[1]/td')[2:]
        time_header = [x.xpath(u"b/text()").extract()[1] for x in time_header]
        print(time_header)

        table = resp.xpath(u'//*[@id="content"]/div/table/tr')[1:]
        for row in table:
            station_id = row.xpath(u"td[2]/text()").extract_first()
            content = row.xpath(u"td")[2:]
            values = [x.xpath(u"font/b/text()").extract_first() for x in content if x.xpath(u"font/b/text()").extract_first() != u"\xa0"]
            current_index = len(values) - 1

            aqi = values[current_index]
            hour = time_header[current_index]
            data_time = u"{day}-{month}-{year} {hour}".format(
                day=resp.meta.get(u"day"),
                month=resp.meta.get(u"month"),
                year=resp.meta.get(u"year"),
                hour=hour
            )

            data_time = parse(data_time).replace(tzinfo=timezone(self.tz))

            station_data = self.aqi_to_value(aqi)

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


