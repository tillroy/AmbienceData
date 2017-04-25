# -*- coding: utf-8 -*-

import re
from datetime import datetime
from pytz import timezone

from scrapy import Spider, Request
from dateutil import parser

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class GpmnSpider(Spider):
    name = u"us_gpmn_weather"
    source = u"https://www.nps.gov"

    @staticmethod
    def get_tz(name):
        tz = {
            (
                'BIBE-KB', 'LIRI-CH', 'LYJO-HB', 'MACA-HM', 'RUCA-VC', 'VOYA-SB', 'COWP-SM',
                'INDU-AB'
            ): 'US/Central',
            (
                'CANY-IS', 'CAVE-MA', 'CHIR-ES', 'CIRO-JC', 'COLM-MY', 'CRMO-VC', 'DETO-JR',
                'DINO-WE',
                'GLAC-WG', 'GRCA-AS', 'GRTE-SS', 'MEVE-RM', 'PEFO-SE', 'ROMO-LP', 'SCBL-VC',
                'YELL-OF',
                'YELL-WT', 'ZION-DW', 'MEEK-PS', 'RANG-GC', 'WALD-CR', 'BADL-VC', 'CHAM-XX',
                'SAGU-EA',
                'THRO-VC', 'WICA-VC', 'YELL-WS'
            ): 'US/Mountain',
            (
                'DEVA-PV', 'GRBA-MY', 'JOTR-BR', 'JOTR-CC', 'JOTR-PW', 'LAVO-ML', 'MOJA-KM',
                'MORA-TW',
                'OLYM-DP', 'PINN-ES', 'SEKI-AS', 'SEKI-LK', 'YOSE-SY', 'YOSE-TD', 'MORA-JV',
                'YOSE-VI'
            ): 'US/Pacific',
            (
                'CUGA-HS', 'EVER-BC', 'GRSM-CD', 'GRSM-CM', 'GRSM-LR', 'KIMO-BM', 'SHEN-BM',
                'ACAD-CM',
                'ACAD-MH', 'CACO-XX', 'COSW-BL', 'EVER-CR', 'GRSM-CC', 'GRSM-PK'
            ): 'US/Eastern',
            ('DENA-HQ',): 'US/Alaska',
            ('HAVO-OB', 'HAVO-VC'): 'US/Hawaii',
        }

        res = None
        for el in tz:
            if name in el:
                res = tz[el]

        return res

    def start_requests(self):
        codes_all = (
            u"BIBE-KB", u"CANY-IS", u"CAVE-MA", u"CHIR-ES", u"CIRO-JC", u"COLM-MY", u"CRMO-VC",
            u"CUGA-HS", u"DEVA-PV", u"DETO-JR", u"DINO-WE", u"EVER-BC", u"GLAC-WG", u"GRCA-AS",
            u"GRTE-SS", u"GRBA-MY", u"GRSM-CD", u"GRSM-CM", u"GRSM-LR", u"JOTR-BR", u"JOTR-CC",
            u"JOTR-PW", u"KIMO-BM", u"LAVO-ML", u"LIRI-CH", u"LYJO-HB", u"MACA-HM", u"MEVE-RM",
            u"MOJA-KM", u"MORA-TW", u"OLYM-DP", u"PEFO-SE", u"PINN-ES", u"ROMO-LP", u"RUCA-VC",
            u"SCBL-VC", u"SEKI-AS", u"SEKI-LK", u"SHEN-BM", u"VOYA-SB", u"YELL-OF", u"YELL-WT",
            u"YOSE-SY", u"YOSE-TD", u"ZION-DW", u"MEEK-PS", u"RANG-GC", u"WALD-CR", u"ACAD-CM",
            u"ACAD-MH", u"BADL-VC", u"CACO-XX", u"CHAM-XX", u"COSW-BL", u"COWP-SM", u"EVER-CR",
            u"GRSM-CC", u"GRSM-PK", u"INDU-AB", u"MORA-JV", u"SAGU-EA", u"THRO-VC", u"WICA-VC",
            u"YELL-WS", u"YOSE-VI", u"DENA-HQ", u"HAVO-OB", u"HAVO-VC")
        codes = (
            u"ACAD-MH", u"BIBE-KB", u"CANY-IS", u"CAVE-MA", u"CHIR-ES", u"CRMO-VC", u"DEVA-PV",
            u"DENA-HQ", u"DINO-WE", u"GLAC-WG", u"GRCA-AS", u"GRTE-SS", u"GRBA-MY", u"GRSM-CC",
            u"GRSM-CD", u"GRSM-CM", u"GRSM-LR", u"JOTR-BR", u"JOTR-CC", u"JOTR-PW", u"LAVO-ML",
            u"MACA-HM", u"MEEK-PS", u"MEVE-RM", u"MOJA-KM", u"PEFO-SE", u"PINN-ES", u"RANG-GC",
            u"ROMO-LP", u"SEKI-AS", u"SHEN-BM", u"THRO-VC", u"VOYA-SB", u"WICA-VC", u"YELL-WT",
            u"YOSE-TD", u"ZION-DW"
        )
        # codes = (u"GRBA-MY",)

        href = u"https://nature.nps.gov/air/data/current/data_{station_id}.cfm"

        for code_value in codes:
            url = href.format(station_id=code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    @staticmethod
    def get_name_and_unit(raw_str):
        is_unit = re.findall(".*\(.*", raw_str)

        if len(is_unit) == 0:
            return raw_str, False
        else:
            name = re.findall("(.+) \(", raw_str)[0]
            unit = re.findall(".*\((.+?)\)", raw_str)[0]
            return name, unit

    def get_station_data(self, resp):
        # //*[@id="datatable"]/tbody/tr[1]/th

        raw_data = resp.xpath(u"//*[@id='datatable'][last()]/tr[last()]/td")

        raw_date = resp.xpath(u"//*[@id='datatable'][last()]/tr[1]/th/text()").extract_first()
        date = " ".join(raw_date.split(" ")[-4:])

        raw_col_names = resp.xpath(u"//*[@id='datatable'][last()]/tr[2]/th")

        poll_values = [el.xpath(u"./text()").extract_first() for el in raw_data]
        raw_poll_names = [self.get_name_and_unit(" ".join(el.xpath(u"./text()").extract())) for el in raw_col_names]
        poll_names = [el[0] for el in raw_poll_names]
        poll_units = [el[1] for el in raw_poll_names]

        data = zip(poll_names, poll_values, poll_units)

        hour = data.pop(0)
        hour = hour[1]

        data_time = " ".join((date, hour))
        data_time = parser.parse(data_time).replace(tzinfo=timezone(self.get_tz(resp.meta[u"code"])))

        units = {
            u"wd": u"cardinals",
        }

        station_data = dict()
        for el in data:
            pollutant_name = el[0]
            pollutant_value = el[1]
            pollutant_units = el[2]

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)

            if pollutant_units is not False:
                pollutant.set_raw_units(pollutant_units)
            else:
                try:
                    pollutant.set_raw_units(units[pollutant.get_name()])
                except KeyError:
                    print(
                    u"There is no such pollutant in local units list <<<<<<<<{0}>>>>>>".format(
                        pollutant.get_name()))

            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items

    def parse(self, response):
        # self.get_station_data(response)
        for el in self.get_station_data(response):
            yield el


