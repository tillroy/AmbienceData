# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider, Selector, Request


class RiotintoSpider(Spider):
    """data update every 10 minutes"""
    # station position
    # http://www.pilbarairon.com/dustmonitoring/

    name = u"au_riotinto"
    source = u"http://www.riotinto.com"
    tz = u"Australia/West"

    def start_requests(self):
        url_codes = (77, 79, 81, 86, 259, 271)
        url = u"http://vdv.benchmarkmonitoring.com.au/vdv/vdv_gmap_site_data.php?site_id={0}"
        for code in url_codes:
            yield Request(
                url=url.format(code)
            )

    def get_date(self, resp):
        row_time = resp.xpath(u'//*[@id="tab1"]/div/text()').extract_first()
        date_str = row_time.lstrip(u"Latest Measurement: ")
        date_parts = date_str.split(u" - ")
        hour = date_parts[0]
        _date_part = date_parts[1].split(u" ")
        date = u".".join((_date_part[0], _date_part[1]))
        data_time = u" ".join((date, hour))
        data_time = parse(data_time).replace(tzinfo=timezone(self.tz))
        return data_time

    def get_station_data(self, resp):
        _station_id = Selector(text=resp.url).re(u"site_id=(\d+)")
        station_id = _station_id[0]

        data_time = self.get_date(resp)
        table = resp.xpath(u'//*[@id="tab1"]/table/tr')

        station_data = dict()
        for row in table:
            col = row.xpath(u"td/text()").extract()
            col = col[:2]

            _name = col[0]
            _val = col[1]
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
        for st in self.get_station_data(response):
            yield st
