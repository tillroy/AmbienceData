# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class QueenslandSpider(Spider):
    """дані оновлюються кожну годину"""
    name = u"au_queensland"
    start_urls = (u"http://www.ehp.qld.gov.au/cgi-bin/air/xml.php?category=1&region=ALL",)
    tz = u"Australia/Queensland"
    source = u"http://www.ehp.qld.gov.au"

    def get_station_data(self, resp):
        stations = resp.xpath(u"//station")
        data_date = resp.xpath(u"category/@measurementdate").extract_first()
        data_hour = resp.xpath(u"category/@measurementhour").extract_first()
        if u"24" == data_hour:
            data_hour = u"00"
        data_time = data_date + u" " + data_hour
        data_time = parse(data_time).replace(tzinfo=timezone(self.tz))

        for st in stations:
            station_id = st.xpath(u"@name").extract_first()
            if u"'" in station_id:
                station_id = station_id.replace(u"'", u"")

            measurements = st.xpath(u"measurement")

            station_data = dict()
            for meas in measurements:
                pol_name = meas.xpath(u"@name").extract_first()
                pol_val = meas.xpath(u"text()").extract_first()
                # print(pol_name, pol_val)

                _tmp_dict = Kind(self.name).get_dict(r_key=pol_name, r_val=pol_val)
                if _tmp_dict:
                    station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            # print(station_data)
            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        for st in self.get_station_data(response):
            yield st
