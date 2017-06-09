# coding: utf-8
from datetime import datetime

from scrapy import Spider, Request
from pytz import timezone
from dateutil import parser
from w3lib.url import add_or_replace_parameter

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ScotlandSpider(Spider):
    name = u"gb_scotland"
    tz = u"GMT"
    source = u"http://www.scottishairquality.co.uk"

    def start_requests(self):
        codes = (u"ABD1", u"ABD", u"AD1", u"ABD0", u"ABD3", u"ABD8", u"ALO2", u"AFR1", u"ACTH", u"BUSH", u"DUMF",
                 u"DUN4", u"DUN6", u"DUN1", u"DUNM", u"DUN5", u"DUN7", u"MARN", u"EDB2", u"EDB1", u"EDB3", u"EDB4",
                 u"MUSS", u"ED11", u"ED10", u"ED5", u"ED9", u"ED8", u"ED1", u"ED3", u"ESK", u"FAL7", u"FAL09", u"FAL11",
                 u"FALK", u"FAL8", u"FAL5", u"FAL3", u"FAL10", u"FAL6", u"CUPA", u"DUNF", u"KIR", u"ROSY", u"FW",
                 u"GL1", u"GLA5", u"GL3", u"GL6", u"GLA6", u"GL9", u"GGWR", u"GHSR", u"GLA4", u"GL2", u"GLKP", u"GLA7",
                 u"GRAN", u"GRA2", u"INC2", u"INV2", u"INV03", u"LERW", u"NL3", u"NL1", u"NL4", u"NL9", u"NL6", u"NL7",
                 u"IRV", u"NL11", u"PAI3", u"PAI4", u"PEEB", u"PET2", u"PET1", u"PETH", u"PET3", u"REN1", u"HARB",
                 u"AYR", u"SL07", u"EK0", u"SL05", u"SL03", u"SLC08", u"SL04", u"SL06", u"STRL", u"SV", u"WDB3",
                 u"WDB4", u"BRX", u"WLC1", u"WLN4")

        # codes = (u"WDB4",)

        url = u"http://www.scottishairquality.co.uk/latest/site-info?view=latest"

        for code_value in codes:
            url = add_or_replace_parameter(url, u"site_id", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        data_time = resp.xpath(u'//*[@id="main"]/div[1]/div[2]/p[3]/text()').re(u"(\d\d\/\d\d\/\d\d\d\d\s\d\d:\d\d)")
        data_time = parser.parse(data_time[0]).replace(tzinfo=timezone(self.tz)) if data_time else None

        table = resp.xpath(u'//*[@id="tabs-content-data"]/table/tbody/tr')

        station_data = dict()
        for row in table:
            pollutant_index = row.xpath(u"td[1]/sub/text()").extract_first() if row.xpath(u"td[1]/sub/text()").extract_first() != None else u""
            pollutant_name = u" ".join((
                row.xpath(u"td[1]/text()").extract_first().split(u" (")[0],
                pollutant_index,
                row.xpath(u"td[4]/text()").extract_first()
            )).replace(u"  ", u" ")

            pollutant_value = row.xpath(u"td[3]/text()").extract_first().split(u" ")[0] if row.xpath(u"td[3]/text()").extract_first().split(u" ")[0] != u"No" else None

            pollutant = Kind(self.name).get_dict(r_key=pollutant_name, r_val=pollutant_value)
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

        # print(table)

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el