# coding: utf-8

import re
from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter, urljoin, url_query_parameter
import pandas as pd
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class CaliforniaSpider(Spider):
    name = u"us_california_pollution"
    source = u"https://www.arb.ca.gov"
    tz = u"US/Pacific"

    def start_requests(self):
        codes = (u"03614", u"04625", u"04633", u"04636", u"04638", u"05633", u"06646", u"07432", u"07434", u"07436",
                 u"07437", u"07441", u"07442", u"07446", u"07447", u"07448", u"09690", u"09691", u"09693", u"09695",
                 u"10025", u"10230", u"10240", u"10241", u"10242", u"10243", u"10244", u"10245", u"10247", u"10248",
                 u"10251", u"10252", u"11676", u"12505", u"12507", u"13601", u"13602", u"13603", u"13604", u"13694",
                 u"13697", u"13698", u"13701", u"13997", u"14022", u"14024", u"14025", u"14151", u"14696", u"14699",
                 u"14700", u"14724", u"14727", u"14728", u"14729", u"14730", u"14732", u"14733", u"14734", u"15242",
                 u"15243", u"15246", u"15248", u"15249", u"15252", u"15255", u"15256", u"15257", u"15258", u"15300",
                 u"15310", u"16716", u"16719", u"17713", u"17720", u"17725", u"17728", u"20211", u"21451", u"21454",
                 u"22742", u"22744", u"22746", u"23754", u"23758", u"23764", u"23769", u"24510", u"24528", u"24531",
                 u"26011", u"26779", u"26785", u"27550", u"27554", u"27555", u"28783", u"29794", u"29800", u"29802",
                 u"30002", u"30031", u"30177", u"30178", u"30195", u"31815", u"31817", u"31818", u"31819", u"31822",
                 u"32821", u"32823", u"32826", u"33031", u"33033", u"33137", u"33144", u"33149", u"33155", u"33157",
                 u"33158", u"33164", u"33165", u"33199", u"33201", u"33601", u"33602", u"34277", u"34279", u"34284",
                 u"34294", u"34295", u"34305", u"34310", u"34311", u"34312", u"34315", u"35823", u"35824", u"36001",
                 u"36035", u"36036", u"36150", u"36151", u"36152", u"36153", u"36155", u"36175", u"36181", u"36197",
                 u"36201", u"36203", u"36204", u"36207", u"36208", u"36220", u"36306", u"39252", u"39271", u"40833",
                 u"40836", u"40844", u"40848", u"40849", u"40850", u"40852", u"40853", u"40990", u"40999", u"41539",
                 u"41541", u"42037", u"42098", u"42101", u"42369", u"42370", u"42381", u"42390", u"42394", u"42395",
                 u"42399", u"42401", u"42402", u"42404", u"42408", u"42409", u"43380", u"43381", u"43383", u"43385",
                 u"43389", u"43393", u"44200", u"44858", u"45555", u"45566", u"45567", u"47861", u"47873", u"47875",
                 u"48879", u"48880", u"48881", u"48884", u"49886", u"49891", u"49895", u"49898", u"49899", u"50568",
                 u"50573", u"51898", u"51899", u"52903", u"52910", u"53915", u"54000", u"54009", u"54036", u"54568",
                 u"55930", u"56172", u"56434", u"56435", u"56436", u"56437", u"56450", u"57570", u"57577", u"57582",
                 u"60337", u"60341", u"60342", u"60344", u"60347", u"60349", u"60351", u"70032", u"70042", u"70043",
                 u"70044", u"70045", u"70046", u"70060", u"70072", u"70073", u"70074", u"70075", u"70087", u"70088",
                 u"70090", u"70091", u"70110", u"70111", u"70112", u"70185", u"70301", u"70591", u"80114", u"80117",
                 u"80128", u"80132", u"80133", u"80135", u"80136", u"80140", u"80142", u"80143", u"80198", u"80201",
                 u"90306")
        # codes = (u"90306", u"80128",)
        codes = (u"80128",)

        href = u"https://www.arb.ca.gov/qaweb/site.php?"

        for code_value in codes:
            url = add_or_replace_parameter(href, u"s_arb_code", code_value)

            yield Request(
                url=url,
                callback=self.get_station_url,
                meta={u"code": code_value}
            )

    @staticmethod
    def validate_url(url):
        try:
            res = url.split("loc_url=")[1]
            res = res.replace("%2F", "/")
            res = res.replace("%3A", ":")
            res = res.replace("%3F", "?")
            res = res.replace("%3D", "=")
            res = res.replace("%26", "&")

            res = res.replace("http://", "https://")
            return res
        except IndexError:
            return None

    def get_station_url(self, resp):

        t = resp.xpath(u'//*[@id="content_area"]').extract()[0]
        all_tables = re.findall(u"<table.*?>(.*?)<\/?table>", t, re.DOTALL)
        raw_rows = re.findall(u"<td.*?>(.+?)<\/td", all_tables[2], re.DOTALL)
        hrefs = re.findall(u"<a href=\"(.+?)\"", raw_rows[0], re.DOTALL)
        # base = u"https://www.arb.ca.gov/qaweb/"
        # raw_links = [urljoin(base, href) for href in hrefs]
        raw_links = [self.validate_url(el) for el in hrefs]
        print("")
        print(raw_links)
        print("")

        # print(resp.url)

        # print(raw_links)
        # get station_id

        # this link redirected to the correct one.
        return Request(
            # start from last
            # url=raw_links.pop(),
            url=raw_links[0],
            # callback=self.get_station_suburl,
            callback=self.get_test,
            meta={
                u"raw_links": raw_links,
                u"links": list(),
                u"code": resp.meta[u"code"]
            }
        )

    def get_station_suburl(self, resp):
        # print("!!!!!!!!!!!!!!", resp.meta["raw_links"])
        try:
            url = resp.xpath(u"/html/head/script/text()").re(u".*window.location = '(.+?)'")[0]
            # print("RES", url)
            # print("RES", resp.meta[u"links"])

            new_links = resp.meta[u"links"]
            new_links.append(url)

            return Request(
                # INFO `pop` method raise an exception
                url=resp.meta[u"raw_links"].pop(),
                callback=self.get_station_suburl,
                meta={
                    u"links": new_links,
                    u"raw_links": resp.meta[u"raw_links"],
                    u"code": resp.meta[u"code"]
                }
            )
        except IndexError:
            print("")
            resp.meta[u"links"] = [el.replace("http://", "https://") for el in resp.meta[u"links"]]
            print("LINKS", resp.meta[u"links"])

            print("")
            print(resp.meta[u"links"].pop())
            return Request(
                # start from last
                url=resp.meta[u"links"].pop(),
                callback=self.get_station_data,
                meta={
                    u"links": resp.meta[u"links"],
                    u"data": None,
                    u"code": resp.meta[u"code"]
                }
            )

    def get_test(self, resp):
        print("")
        print("TEST")
        print(resp.url)


    def get_station_data(self, resp):
        print("")
        print("LINKS", resp.url)
        print("")

        try:
            pollutant_name = url_query_parameter(resp.url, u"param")
            unit = None

            raw_data = resp.xpath(u".//*[@id='graph']/table/tr[1]/td").extract_first()

            try:
                # print(raw_data)
                raw_date = re.findall(u"(\d\d\/\d\d\/\d\d\d\d)", raw_data)[0] if raw_data is not None else None
            except IndexError:
                raw_date = None


            regexs = (u"\(\s*(.+)\s+\)", u"degrees/(.+)")
            # print(u"TEST", raw_data)
            if raw_data is not None:
                raw_units = list()
                for reg in regexs:
                    _unit = re.findall(reg, raw_data)
                    raw_units.extend(_unit)
                if len(raw_units):
                    unit = re.sub(u"<.+?>", u"", raw_units[0])
                else:
                    print(u"\nCan't find UNIT\nfrom: <{1}>\n".format(resp.url))

            # print("DATA", pollutant_name, raw_date, unit)
            raw_hours = resp.xpath(u".//*[@id='graph']/table/tr[3]/td")[2:]

            data_time_list = list()
            for el in raw_hours:
                try:
                    hour = el.xpath(u"div/text()").re(u"\s*(\d\d)\s*-")[0]
                except IndexError:
                    hour = None

                if hour is not None and raw_data is not None:
                    data_time = u" ".join((raw_date, hour + u":" + u"00"))
                    data_time_list.append(data_time)
                else:
                    data_time = None
                    data_time_list.append(data_time)

            raw_values = resp.xpath(u".//*[@id='graph']/table/tr[4]/td")[2:]
            values = [el.xpath(u"div/span/text()").extract_first() for el in raw_values]
            values = [el.replace(u"CALM", u"0") if el is not None else None for el in values]

            new_values = list()
            for el in values:
                if el is not None:
                    if u"/" in el:
                        try:
                            _val = el.split(u"/")[1]
                        except IndexError:
                            _val = None
                    else:
                        _val = el

                    new_values.append(_val)
                else:
                    new_values.append(None)

            values = new_values

            df = pd.DataFrame({u"date": data_time_list, u"value": values})
            df[u"unit"] = unit
            df[u"name"] = pollutant_name
            # print(df)
            res = df.dropna(0)
            res = res.tail(1).copy()

            if resp.meta.get(u"data") is None:
                resp.meta[u"data"] = res
            else:
                # print(res.values[0])
                data = pd.concat([resp.meta[u"data"], res], ignore_index=True)
                # print(data)
                resp.meta[u"data"] = data

            return Request(
                url=resp.meta[u"links"].pop(),
                callback=self.get_station_data,
                meta={
                    u"links": resp.meta.get(u"links"),
                    u"data": resp.meta[u"data"],
                    u"code": resp.meta[u"code"]
                }
            )
        except IndexError:
            data = resp.meta[u"data"]
            # print(data)
            data[u"date"] = [parser.parse(x) for x in data[u'date']]

            current_data_time = data[u"date"].max()
            curr_data = data[data[u"date"] == current_data_time]
            curr_data = curr_data[[u"name", u"value", u"unit"]]

            station_data = dict()
            for el in curr_data.itertuples(index=False):
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
                items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = resp.meta[u"code"]

                return items
