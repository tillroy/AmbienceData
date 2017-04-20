# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from lxml import etree
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class WellingtonSpider(Spider):
    name = u"nz_wellington"
    tz = u"NZ"
    source = u"http://www.gw.govt.nz"
    start_urls = (u"http://hilltop.gw.govt.nz/Data.hts?Service=Hilltop&Request=GetData&Collection=Air%20Quality",)

    def get_station_data(self, resp):
        body = resp.body
        root = etree.fromstring(body)
        row_data = root.xpath(u"//Measurement")

        station = dict()
        date = root.xpath(u"//Measurement[1]/Data/E/T/text()")
        data_time = parse(date[0]).replace(tzinfo=timezone(self.tz))

        for st in row_data:
            name = st.xpath(u"@SiteName")
            pol_name = st.xpath(u"DataSource/@Name")
            pol_val = st.xpath(u"Data/E/I1/text()")
            pol_time = st.xpath(u"Data/E/T/text()")
            print(name, pol_name, pol_val, pol_time)

            if name[0] not in station:
                station[name[0]] = dict()

            _tmp_dict = Kind(self.name).get_dict(r_key=pol_name[0], r_val=pol_val[0])
            if _tmp_dict:
                station[name[0]][_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        for st_data in station:
            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station[st_data]
                items[u"source"] = self.source
                items[u"source_id"] = st_data

                yield items

    def parse(self, response):
        for st in self.get_station_data(response):
            yield st
