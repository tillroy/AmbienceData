# coding: utf-8
from datetime import datetime
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class NewBrunswickSpider(Spider):
    name = u"ca_new_brunswick"
    tz = u"Canada/Newfoundland"
    source = u"http://www.elgegl.gnb.ca"
    st_data = (
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/1?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/2?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/3?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/14?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/15?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/16?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/17?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/18?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/21?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/22?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/27?type=1",
        u"http://www.elgegl.gnb.ca/AirNB/en/SamplingLocation/Details/30?type=1"
    )
    start_urls = st_data

    def check_date(self, row_dt_hour):
        try:
            row_dt_hour = row_dt_hour.split(u":")
            row_dt_now = datetime.today()
            dt = row_dt_now.replace(hour=int(row_dt_hour[0]), minute=int(row_dt_hour[1]), second=0)
            new_dt = dt.replace(tzinfo=timezone(self.tz))
        except IndexError:
            new_dt = None

        return new_dt

    def get_st_data(self, resp):
        regex = u"Details/(.+)\?type=1"
        st_id = findall(regex, resp.url)
        st_id = str(st_id[0])

        row_data = resp.xpath(u'//*[@id="recentResults"]/tbody/tr')
        row_dt_hour = resp.xpath(u'//*[@id="recentResults"]/thead/tr[2]/th[1]/text()').re_first(u"Current \((.+)\)")
        new_dt = self.check_date(row_dt_hour)

        st_data = dict()
        for data in row_data:
            _name = data.xpath(u"td[1]/text()").re_first(u"(.+[\S])")
            _val = data.xpath(u"td[2]/text()").re_first(u"(.+[\S])")

            _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
            if _tmp_dict:
                st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        if st_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = new_dt
            items[u"data_value"] = st_data
            items[u"source"] = self.source
            items[u"source_id"] = st_id

            yield items

    def parse(self, response):
        for st in self.get_st_data(response):
            yield st
