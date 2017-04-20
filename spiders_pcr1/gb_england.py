# coding: utf-8
from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
from w3lib.url import add_or_replace_parameter

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class EnglandSpider(Spider):
    name = u"gb_england"
    tz = u"GMT"
    source = u"http://www.airqualityengland.co.uk"

    def start_requests(self):
        codes = (u"AY1", u"BAR6", u"BAR9", u"HB010", u"HB011", u"BAI2", u"WIL1", u"BUR2", u"BUR1", u"WIL8", u"WIL5",
                 u"NEW2", u"CAM3", u"CAM5", u"CAM4", u"CAM1", u"CRL2", u"HB013", u"WIL3", u"HB012", u"EWE2", u"FAR2",
                 u"GA1", u"GA2", u"GA3", u"GIRT", u"FAR1", u"T55", u"LHR2", u"T54", u"HEN", u"HB008", u"HB009", u"HI1",
                 u"SIPS", u"HB002", u"HB003", u"HS5", u"HS4", u"HS2", u"HS9", u"HS8", u"HS7", u"HS6", u"BN2", u"HIL1",
                 u"HIL4", u"HIL5", u"HI3", u"HB006", u"HB007", u"MAN1", u"MAN7", u"MAHG", u"WIL7", u"NUL1", u"OX6",
                 u"OX3", u"REA2", u"REA4", u"RED3", u"IMP", u"ORCH", u"M60", u"WIL4", u"CW", u"SLH7", u"SLH3", u"SLH6",
                 u"SLH5", u"SLH8", u"SLH9", u"SLH4", u"GX", u"SHOL", u"MONK", u"HB005", u"STK7", u"STK5", u"SUN2",
                 u"SUN4", u"BN1", u"TAM1", u"TAME", u"GOS1", u"TRAF", u"TRF2", u"WD1", u"WL4", u"WL1", u"WL5", u"HB004",
                 u"WAT", u"HB001", u"WID2", u"WID1", u"WIG7", u"NEW3", u"WYA4", u"WSTO", u"YK10", u"YK11", u"YK16",
                 u"YK7", u"YK13", u"YK8", u"YK9", u"YK15", u"YK018", u"BAR3", u"BPLE", u"BATH", u"BIL", u"BBRD",
                 u"BIRR", u"AGRN", u"BIR1", u"BLAR", u"BLC2", u"BORN", u"BDMA", u"BRT3", u"BRS8", u"BURW", u"CAM",
                 u"CANK", u"CANT", u"CARL", u"MACK", u"CHAT", u"CHLG", u"CHS7", u"CHBO", u"CHBR", u"COAL", u"DCST",
                 u"EB", u"EX", u"GLAZ", u"HM", u"HONI", u"HUL2", u"HULR", u"LB", u"LEAM", u"LEAR", u"LEED", u"LED6",
                 u"LEIR", u"LECU", u"LEOM", u"LIN3", u"LVP", u"LH", u"LUTR", u"MAN3", u"MKTH", u"MID", u"NEWC", u"NCA3",
                 u"NTN3", u"NO12", u"NOTT", u"NWBV", u"BOLD", u"OX", u"OX8", u"PLYM", u"PMTH", u"PRES", u"REA5",
                 u"ROCH", u"ECCL", u"SASH", u"SDY", u"SCN2", u"SHBR", u"SHDG", u"SHE", u"SIB", u"SA33", u"SOUT",
                 u"SEND", u"SHLW", u"OSY", u"SOTR", u"EAGL", u"STKR", u"STOK", u"STOR", u"SUNR", u"WAL4", u"WAR",
                 u"WEYB", u"WFEN", u"WSMR", u"WIG5", u"TRAN", u"WTHG", u"YW")

        # codes = (u"LEIR",)
        url = u"http://www.airqualityengland.co.uk/site/latest"
        for code_value in codes:
            url = add_or_replace_parameter(url, u"site_id", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        data_time = resp.xpath(u'//*[@id="pageSubArea"]/div/p[1]/text()').re(u"(\d\d\/\d\d\/\d\d\d\d\s\d\d:\d\d)")
        data_time = parser.parse(data_time[0]).replace(tzinfo=timezone(self.tz)) if data_time else None

        table = resp.xpath(u'//*[@id="pageSubArea"]/div/table/tr')[1:]

        station_data = dict()
        for row in table:
            pollutant_name = row.xpath(u'td[1]/text()').extract_first().split(u" (")[0]
            pollutant_name_ind = row.xpath(u'td[1]/sub/text()').extract_first() if row.xpath(u'td[1]/sub/text()').extract_first() != None else u""
            pollutant_name_time = row.xpath(u"td[last()]/text()").extract_first()

            pollutant_name = (
                u" ".join((pollutant_name, pollutant_name_ind, pollutant_name_time))
            ).replace(u"  ", u" ")

            pollutant_value = row.xpath(u"td[last() - 1]/text()").extract_first()

            if pollutant_value is not None:
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
