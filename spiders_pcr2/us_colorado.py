# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from w3lib.url import add_or_replace_parameter
from dateutil import parser
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ColoradoSpider(Spider):
    name = u"us_colorado"
    source = u"http://www.colorado.gov"
    tz = u"US/Mountain"

    # for station location and IDs
    # start_urls = (u"http://www.colorado.gov/airquality/site_description.aspx",)

    def start_requests(self):
        codes = (u"080010008", u"080590002", u"080970007", u"080590013", u"080970008", u"080050006", u"080130014",
                 u"080131001", u"080310002", u"080310014", u"080350004", u"080410017", u"080771001", u"080830006",
                 u"080290004", u"080310025", u"080310016", u"080810003", u"080691004", u"080690009", u"80690012",
                 u"080690012", u"080690011", u"080770017", u"080770018", u"081230006", u"081230009", u"080050002",
                 u"080410015", u"080310027", u"080310028", u"080310026", u"080990003", u"080130003", u"080410016",
                 u"080310013", u"080590011", u"080770020", u"080850005", u"080450012", u"080450007", u"080590006",
                 u"080410013", u"080013001", u"080590005")
        # codes = (u"080310002",)

        href = u"http://www.colorado.gov/airquality/site.aspx?"
        for code_value in codes:
            url = add_or_replace_parameter(href, u"aqsid", code_value)

            yield Request(
                url=url,
                callback=self.parse,
                meta={u"code": code_value}
            )

    def get_station_location(self, resp):
        table = resp.xpath(u'//*[@id="divSites"]/p')
        for station_data in table:
            station_name = station_data.xpath(u"b/text()").extract_first()
            station_id = station_data.xpath(u"./text()").re(u"AQS ID:\s*(.*)")[0]
            lat = station_data.xpath(u"./text()").re(u"Latitude:\s*(.*)")[0]
            lon = station_data.xpath(u"./text()").re(u"Longitude:\s*(.*)")[0]
            address = station_data.xpath(u"./text()").extract()
            address = str(address[2])

            res = u";".join((station_id, station_name, address, lon, lat, u"\n"))

            open(u"us_colorado_stations.txt", u"a").write(res.encode())

    def get_station_data(self, resp):
        #  get date from title
        raw_data_date = resp.xpath(u'//*[@id="pTitle"]/text()').re(u'(\d+[/. ]?\d+[/. ]?\d+)')[0]

        raw_table = resp.xpath(u'//*[@id="divSummaryData"]/table//tr')
        raw_poll_names = raw_table[0].xpath(u"td")[1:]
        poll_names = [el.xpath(u"./text()").re(u"(.+?)\s+")[0] for el in raw_poll_names]
        poll_units = map(u"".join, [el.xpath(u"./div/text()").re(u"\s*(.+?)\s*") for el in raw_poll_names])

        tmp_data = list()
        for row in raw_table[1:]:

            raw_row = row.xpath(u"td")
            new_row = [el.xpath(u"./text()").extract_first() for el in raw_row]

            if sum(x is not None for x in new_row) > 1:
                tmp_data.append(new_row)

        curr_all_data = tmp_data[len(tmp_data) - 1]

        raw_data_hour = curr_all_data[0]
        raw_data_time = u" ".join((raw_data_date, raw_data_hour))
        data_time = parser.parse(raw_data_time).replace(tzinfo=timezone(self.tz))

        poll_values = curr_all_data[1:]

        data = zip(poll_names, poll_values, poll_units)

        station_data = dict()
        for record in data:
            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            # print("record", record)
            pollutant.set_raw_name(record[0])
            pollutant.set_raw_value(record[1])
            pollutant.set_raw_units(record[2])
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
        # self.get_station_location(response)
        for el in self.get_station_data(response):
            yield el
