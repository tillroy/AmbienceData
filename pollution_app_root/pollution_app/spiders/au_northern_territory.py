# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class NorthernTerritory(Spider):
    name = u"au_northern_territory"
    tz = u"Australia/Darwin"
    source = u"http://www.ntepa.nt.gov.au"
    st_data = (
        u"http://ntepa.webhop.net/NTEPA/StationInfo.aspx?ST_ID=1",
        u"http://ntepa.webhop.net/NTEPA/StationInfo.aspx?ST_ID=2"
    )
    start_urls = st_data

    def get_st_data(self, resp):
        regex = u".*ST_ID=(.+)"
        station_id = findall(regex, resp.url)
        station_id = str(station_id[0])

        # формужмо значення назв забрудників таблиці
        row_col_names = resp.xpath(u'//*[@id="C1WebGrid1"]/tbody/tr[1]/td/div/text()').extract()
        col_names = list()
        for col_name in row_col_names:
            col_name = col_name.lstrip(u'\n\t')
            col_name = col_name.rstrip(u'\n\t')
            col_names.append(col_name)

        col_names = col_names[1:]

        # витягуємо значення забрудників
        row_data = resp.xpath(u'//*[@id="C1WebGrid1"]/tbody/tr[5]/td')
        data_values = list()
        for data in row_data:
            row_value = data.xpath(u"div/text()").re(u"([\S].+[\S])")
            try:
                data_values.append(row_value[0])
            except IndexError:
                data_values.append(None)

        # значення дати даних
        data_date = parse(data_values[0])
        data_date = data_date.replace(tzinfo=timezone(self.tz))

        data_values = data_values[1:]
        data = zip(col_names, data_values)

        station_data = dict()
        for st in data:
            _name = st[0]
            _val = st[1]
            _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
            if _tmp_dict:
                station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_date
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = station_id

            return items

    def parse(self, response):
        yield self.get_st_data(response)
