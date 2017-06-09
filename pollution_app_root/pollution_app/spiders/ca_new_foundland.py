# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class NewFoundlandSpider(Spider):
    name = u"ca_new_foundland"
    tz = u"Canada/Newfoundland"
    source = u"http://www.env.gov.nl.ca"
    st_data = (
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/PortAuxChoix_Line.xml",
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/CornerBrook_Line.xml",
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/GrandFallsWindsor_Line.xml",
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/Burin_Line.xml",
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/MtPearl_Line.xml",
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/StJohns_Line.xml",
        u"http://www.env.gov.nl.ca/wrmd/PP_ADRS/Data/FireHall_Line.xml"
    )

    start_urls = st_data

    def check_date(self, row_date):
        row_date = str(row_date)
        dt = parse(row_date)
        new_dt = dt.replace(tzinfo=timezone(self.tz))
        return new_dt

    def get_st_data(self, resp):
        regex = u"Data/(.+)_Line\.xml"
        st_id = findall(regex, resp.url)
        st_id = str(st_id[0])

        row_data = resp.xpath(u"tname[1]/child::*")
        row_dt = resp.xpath(u"tname[1]/DATE_TIME/text()").extract_first()
        new_dt = self.check_date(row_dt)

        st_data = dict()
        for el in row_data:
            tag_name = el.xpath(u"name()").extract_first()
            tag_val = el.xpath(u"text()").extract_first()
            _name = tag_name
            _val = tag_val

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
