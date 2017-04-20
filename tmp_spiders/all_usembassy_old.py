# coding: utf-8
from scrapy import Spider
from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from dateutil.parser import parse
from pytz import timezone
from pollution_app.settings import SCRAPER_TIMEZONE
from datetime import datetime


class USembassySpider(Spider):
    name = u"all_usembassy_old"
    source_name = u"http://newdelhi.usembassy.gov"
    # start_urls = (u"http://newdelhi.usembassy.gov/airqualitydataemb.html",)
    # AIRNOW department https://www.airnow.gov/index.cfm?action=airnow.global_summary#India$New_Delhi
    # start_urls = (u"http://stateair.net/dos/RSS/NewDelhi/NewDelhi-PM2.5.xml",)
    start_urls = (u"http://stateair.net/dos/AllPosts24Hour.json",)
    tz = u"Asia/Kolkata"

    def parse_date(self, row_datatime):
        data_time = parse(row_datatime)
        data_time = data_time.replace(tzinfo=timezone(self.tz))
        return data_time

    def get_station_data(self, resp):
        row_rows = resp.xpath(u"/html/body/div[7]/div/div[1]/div[2]/div/div[3]/table/tbody/tr[2]")
        print(row_rows)
        station_name = row_rows.xpath(u"td[1]/b/text()").extract_first()
        station_id = station_name
        aqi = row_rows.xpath(u"td[2]/p/text()").extract_first()
        pm25 = row_rows.xpath(u"td[3]/p/text()").extract_first()
        row_data_time = row_rows.xpath(u"td[4]/span/text()").extract_first()
        print(row_data_time)
        data_time = self.parse_date(row_data_time)

        _tmp_dict = Kind(self.name).get_dict(r_key=u"pm25", r_val=pm25)
        station_data = dict()
        if _tmp_dict:
            station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        _tmp_dict = Kind(self.name).get_dict(r_key=u"aqi", r_val=aqi)
        if _tmp_dict:
            station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source_name
            items[u"source_id"] = str(station_id)

            yield items

    def parse(self, response):
        for station in self.get_station_data(response):
            yield station
