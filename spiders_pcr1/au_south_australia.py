# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


class SouthAustralia(Spider):
    name = u"au_south_australia"
    tz = u"Australia/South"
    source = u"http://www.epa.sa.gov.au"
    st_data = (u"http://www.epa.sa.gov.au/legacy/air_quality_index.php?page=319#daily",)
    start_urls = st_data

    def get_date(self, resp):
        date = resp.xpath(u'//*[@id="column-main"]/h3/text()').extract_first()
        date = date.strip(u"EPA Air Quality summary : ")
        date_parts = date.split(u", ")
        date_parts[1] = date_parts[1].strip(u"Data up to ")
        date_parts[1] = date_parts[1].strip(u"cst")
        date = u" ".join(date_parts)
        date = parse(date).replace(tzinfo=timezone(self.tz))

        return date

    def get_st_data(self, resp):
        data_date = self.get_date(resp)

        col_names = resp.xpath(u'//*[@id="column-main"]/table/tr[1]/th/text()').extract()
        value_names = list()
        for name in col_names:
            value_names.append(name)
        value_names = value_names[2:]

        table = resp.xpath(u'//*[@id="column-main"]/table/tr')
        table = table[1:]
        for row in table:
            col = row.xpath(u"td/text()").extract()
            st_id = col[0].strip(u"\xa0")

            col = col[2:]
            data_values = list()
            for value in col:
                data_values.append(value)
            # print(data_values)
            # print len(data_values)

            data = zip(value_names, data_values)

            st_data = dict()
            for st in data:
                _name = st[0]
                _val = st[1]
                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_date
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = st_id

                yield items

    def parse(self, response):
        for st in self.get_st_data(response):
            yield st
