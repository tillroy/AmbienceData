#  coding: utf-8
import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

import re
import scrapy
from pollution_app.pollution import Kind
from scrapy.http import Request


class OntarioSpider(scrapy.Spider):
    name = u"ca_ontario"
    tz = u"EST"
    source = u"http://www.airqualityontario.com"
    start_urls = (u"http://www.airqualityontario.com/history/summary.php",)

    def get_st_pos(self, resp):
        base = u"http://www.airqualityontario.com"
        table = resp.xpath(u'//*[@id="right_column"]/div/table/tbody/tr')
        for row in table:
            cols = row.xpath(u"td")
            href = cols[0].xpath(u"div/a/@href").extract_first()
            url = base + href

            yield Request(url, callback=self.make_pos_sql_file)
            # yield Request(url, callback=self.parse_station)

    @staticmethod
    def parse_station(resp):
        regex_station = u"stationid=(.+)"
        st_id = re.findall(regex_station, resp.url)

        table = resp.xpath(u'//*[@id="right_column"]/div[2]/div[2]/div/table[1]/tbody/tr')
        st_info = {
            u"source": u"http://www.airqualityontario.com",
        }

        for row in table:
            col = row.xpath(u"td/text()").extract()

            if st_id:
                st_info[u"source_id"] = str(st_id[0])
            else:
                st_info[u"source_id"] = None

            if col[0] == u"Latitude:":
                if col[1]:
                    st_info[u"lat"] = float(col[1])
                else:
                    st_info[u"lat"] = None

            if col[0] == u"Longitude:":
                if col[1]:
                    st_info[u"lon"] = float(col[1])
                else:
                    st_info[u"lon"] = None

            if col[0] == u"Address:":
                if col[1]:
                    st_info[u"address"] = str(col[1])
                else:
                    st_info[u"address"] = u"-"

            if col[0] == u"Station Name:":
                if col[1]:
                    st_info[u"st_name"] = str(col[1])
                else:
                    st_info[u"st_name"] = u"-"

        return st_info
        # print(st_info)

    def make_pos_sql_file(self, resp):
        st_info = self.parse_station(resp)
        if (st_info[u"lon"] is not None or st_info[u"lat"] is not None) and st_info[u"source_id"] is not None:
            open(u"add_map_ca_ontario.sql", u"a").write("""
                INSERT INTO scrapper_map(lon, lat, address, source_id, source, st_name)
                VALUES({0},{1},{2},{3},{4},{5});
                """.format(
                    st_info[u"lon"],
                    st_info[u"lat"],
                    """ + str(st_info["address"]) + """,
                    """ + str(st_info["source_id"]) + """,
                    """ + str(st_info["source"]) + """,
                    """ + str(st_info["st_name"]) + """)
                    )

    def get_st_data(self, resp):
        table = resp.xpath(u'//*[@id="right_column"]/div/table/tbody/tr')
        col_names = resp.xpath(u'//*[@id="right_column"]/div/table/thead//th/@abbr').extract()

        data_date = resp.xpath(u'//*[@id="right_column"]/div/h1/text()').extract()
        data_date = str(data_date[0]).lstrip(u"Pollutant Concentrations for ")
        dt = parse(data_date)
        new_dt = dt.replace(tzinfo=timezone(self.tz))

        #  get correct values, if there is no value add ""
        for row in table:
            cols = row.xpath(u"td")

            #  get id from href
            url = cols[0].xpath(u"div/a/@href").extract()
            url = str(url[0])
            regex_station = u"stationid=(.+)"
            st_id = re.findall(regex_station, url)
            st_id = str(st_id[0])

            row_data = []
            for col in cols:
                text = col.xpath(u"div[1]/text()").extract()
                try:
                    row_data.append(text[0])
                except IndexError:
                    row_data.append(None)

            gen_data = zip(col_names, row_data)
            st_data = dict()
            for data in gen_data:
                _key = data[0]
                _val = data[1]

                _tmp_dict = Kind(self.name).get_dict(r_key=_key, r_val=_val)

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
        for item in self.get_st_data(response):
            yield item
