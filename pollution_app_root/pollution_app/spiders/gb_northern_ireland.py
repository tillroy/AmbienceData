# coding: utf-8
from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class NorthernIrelandSpider(Spider):
    name = u"gb_northern_ireland"
    tz = u"GMT"
    source = u"http://www.airqualityni.co.uk"

    def start_requests(self):
        codes = (u"ARM5", u"BALY", u"BEL2", u"BEL5", u"BEL0", u"BEL1", u"BE2", u"CAS3", u"DER4", u"DERR", u"DPK1",
                 u"LVD2", u"LIS5", u"LN", u"NWY5", u"NWT5", u"ND2", u"SBAN")

        url = u"http://www.airqualityni.co.uk/ajax/site-tabs?doajax=true&view=latest"

        for code_value in codes:
            url = add_or_replace_parameter(url, u"site_id", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        data_time = resp.xpath(u'//*[@id="tabs-content-data"]/p/b/text()').re(u"(\d\d\/\d\d\/\d\d\d\d\s\d\d:\d\d)")
        data_time = parser.parse(data_time[0]).replace(tzinfo=timezone(self.tz)) if data_time else None

        table = resp.xpath(u'//*[@id="tabs-content-data"]/table/tbody/tr')

        station_data = dict()
        for row in table:
            pollutant_index = row.xpath(u"td[1]/sub/text()").extract_first() if row.xpath(u"td[1]/sub/text()").extract_first() != None else u""

            pollutant_name = u" ".join(
                (row.xpath(u"td[1]/text()").extract_first().split(u" (")[0],
                 pollutant_index,
                 row.xpath(u"td[4]/text()").extract_first()))
            pollutant_name = pollutant_name.replace(u"  ", u" ")

            value = row.xpath(u"td[3]/text()").extract_first().split(u" ")[0] if row.xpath(u"td[3]/text()").extract_first().split(u" ")[0] != u"No" else None

            pollutant = Kind(self.name).get_dict(r_key=pollutant_name, r_val=value)
            if pollutant:
                station_data[pollutant[u"key"]] = pollutant[u"val"]

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
