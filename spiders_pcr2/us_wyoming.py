# coding: utf-8

import re
from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter, url_query_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class WyomingSpider(Spider):
    name = u"us_wyoming"
    source = u"http://www.wyvisnet.com"
    tz = u"US/Mountain"

    def start_requests(self):
        codes = (u"MURI1", u"MOXA1", u"WYHI1", u"WAMS1", u"SOPA1", u"WYJS1", u"WYBP1", u"SHEL1", u"WYPD1", u"WYCH1",
                 u"CHEY1", u"CASP1", u"WYCA1", u"WYCV1", u"CACO1", u"TBNG1")
        # codes = (u"MURI1",)

        href = u"http://www.wyvisnet.com/Sites/Site.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(href, u"site", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_data(self, resp):
        try:
            data_time = resp.xpath(u".//*[@id='Content_cphMain_pnLastUpdate']/p/text()").re(u"Last Updated on (.+)")[0]
            data_time = parser.parse(data_time).replace(tzinfo=timezone(self.tz))
        except IndexError:
            data_time = None

        raw_poll_data = resp.xpath(u".//*[@id='Content_cphMain_dlAirQualityParameters']/tr/td")
        data = list()
        for el in raw_poll_data:
            poll_name_part = el.xpath(u"h6/text()").extract_first()
            poll_name_part_add = el.xpath(u"h6/sub/text()").extract_first()
            poll_name_part = u"".join((poll_name_part, poll_name_part_add)) if poll_name_part_add is not None else poll_name_part
            poll_name_part = u" ".join(poll_name_part.split())
            # print(poll_name_part)

            _data = el.xpath(u'div[not(@class="clearfix")]')
            _data = [el.xpath(u"text()").extract_first() for el in _data]

            _data = [u" ".join(el.split()) for el in _data]
            _data = [el for el in _data if el != u""]
            poll_subnames = _data[::2]
            poll_names = [" ".join((poll_name_part, el)) for el in poll_subnames]

            raw_poll_values = _data[1::2]

            poll_values = list()
            poll_units = list()
            for el in raw_poll_values:
                value = re.findall(u"^([-]?\d*[.]?\d+|\d+[.]?\d*)", el)[0]
                unit = re.sub(u"^([-]?\d*[.]?\d+|\d+[.]?\d*)", u"", el)
                if unit is not None:
                    unit = u" ".join(unit.split())

                poll_values.append(value)
                poll_units.append(unit)
            subdata = zip(poll_names, poll_values, poll_units)
            data.extend(subdata)

        raw_weather_data = resp.xpath(u".//*[@id='Content_cphMain_gvMeteorology']/tr")

        wind_dir = {
            u"N": u"0",
            u"NNE": u"22.5",
            u"NE": u"45",
            u"ENE": u"68.5",
            u"E": u"90",
            u"ESE": u"112.5",
            u"SE": u"135",
            u"SSE": u"157.5",
            u"S": u"180",
            u"SSW": u"202.5",
            u"SW": u"225",
            u"WSW": u"247.5",
            u"W": u"270",
            u"WNW": u"292.5",
            u"NW": u"315",
            u"NNW": u"337.5",
        }
        w_data = list()
        for el in raw_weather_data:
            name = el.xpath(u"td[1]/text()").extract_first()
            name = u" ".join(name.split()) if name is not None else None

            raw_poll_val_1 = u" ".join(el.xpath(u"td[2]/text()").extract())
            raw_poll_val_2 = u" ".join(el.xpath(u"td[2]/sup/text()").extract())
            raw_poll_val = u" ".join((raw_poll_val_1, raw_poll_val_2))
            raw_poll_val = u" ".join(raw_poll_val.split())
            if wind_dir.get(raw_poll_val) is not None:
                raw_poll_val = wind_dir.get(raw_poll_val) + u" deg"

            value = re.findall(u"^([-]?\d*[.]?\d+|\d+[.]?\d*)", raw_poll_val)[0]
            unit = re.sub(u"^([-]?\d*[.]?\d+|\d+[.]?\d*)", u"", raw_poll_val)
            unit = u" ".join(unit.split())

            res = (name, value, unit)
            w_data.append(res)

        data.extend(w_data)
        # print(data)
        station_data = dict()
        for el in data:
            pollutant_name = el[0]
            pollutant_value = el[1]
            pollutant_units = el[2]

            # print(pollutant_name, pollutant_value, pollutant_units)

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            pollutant.set_raw_units(pollutant_units)
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
        for el in self.get_station_data(response):
            yield el