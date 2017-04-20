# coding: utf-8
from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class WalesSpider(Spider):
    name = u"gb_wales"
    tz = u"GMT"
    source = u"http://www.welshairquality.co.uk"

    def start_requests(self):
        codes = (u"ANG5", u"AN10", u"ANG2", u"AH", u"CAE5", u"CAE8", u"CAE4", u"CARD", u"CHEP", u"NTH1", u"CWMB",
                 u"CAE6", u"MAWR", u"CAE7", u"PEMB", u"NPT4", u"NPT1", u"PT7", u"PT6", u"PTLW", u"PT4", u"PT11", u"PT8",
                 u"PT10", u"PT9", u"RHD7", u"RHD8", u"RHD6", u"RHD4", u"RHD2", u"SWA9", u"SWA7", u"SWA5", u"SWA1",
                 u"SWA8", u"SW10", u"TWYN", u"GLM8", u"WREX")

        # codes = (u"CAE7 ",)

        url = u"http://www.welshairquality.co.uk/ajax_process/show_site_tabs.php?doajax=true&t_action=data&pagename=undefined&lg=e"
        for code_value in codes:
            url = add_or_replace_parameter(url, u"site_id", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        data_time = resp.xpath(u'//*[@id="tab_content"]/p[1]/strong/text()').re(u"(\d\d\/\d\d\/\d\d\d\d\s\d\d:\d\d)")
        data_time = parser.parse(data_time[0]).replace(tzinfo=timezone(self.tz)) if data_time else None

        table = resp.xpath(u'//*[@id="tab_content"]/table/tr')[1:]

        station_data = dict()
        for row in table:
            pollutant_name = row.xpath(u"td[1]/text()").extract_first()
            pollutant_name_ind = row.xpath(u"td[1]/sub/text()").extract_first() if row.xpath(u"td[1]/sub/text()").extract_first() != None else u""
            pollutant_name_time = row.xpath(u"td[last()]/text()").extract_first()

            pollutant_name = (
                u" ".join((pollutant_name, pollutant_name_ind, pollutant_name_time))
            ).replace(u"  ", u" ")

            pollutant_value = row.xpath(u"td[last() - 1]/text()").extract_first()
            if u"\xa0" in pollutant_value:
                pollutant_value = pollutant_value.split(u"\xa0")[0]
            else:
                pollutant_value = pollutant_value.split(u" ")[0]

            pollutant_value = pollutant_value if pollutant_value != u"No" else None

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

    def parse(self, response):
        for el in self.get_station_data(response):
            yield el
