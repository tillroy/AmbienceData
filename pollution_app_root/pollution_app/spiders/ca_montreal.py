# coding: utf-8
import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

import scrapy
from pollution_app.pollution import Kind


class MontrealSpider(scrapy.Spider):
    name = u"ca_montreal"
    tz = u"America/Montreal"
    source = u"http://ville.montreal.qc.ca"
    start_urls = (u"http://ville.montreal.qc.ca/rsqa/servlet/makeXmlActuel",)

    def get_st_data(self, resp):
        stations = resp.xpath(u"//station")
        st_date = resp.xpath(u"./journee")
        st_year = st_date.xpath(u"@annee").extract_first()
        st_month = st_date.xpath(u"@mois").extract_first()
        st_day = st_date.xpath(u"@jour").extract_first()
        st_date = st_year + u"-" + st_month + u"-" + st_day

        for st in stations:
            last_st = st.xpath(u"echantillon[last()]")
            data = last_st.xpath(u"polluant")
            st_id = st.xpath(u"@id").extract_first()
            st_hour = last_st.xpath(u"@heure").extract_first()
            new_dt = st_date + u" " + st_hour + u":00"
            new_dt = parse(new_dt)
            new_dt = new_dt.replace(tzinfo=timezone(self.tz))
            # print(new_dt)

            st_data = dict()
            for pol in data:
                _name = pol.xpath(u"@nom").extract_first()
                _val = pol.xpath(u"@value").extract_first()

                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = new_dt
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = st_id

                yield items

    def parse(self, response):
        for st in self.get_st_data(response):
            yield st
