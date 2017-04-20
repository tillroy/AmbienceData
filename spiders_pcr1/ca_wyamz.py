# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class WyamzSpider(Spider):
    name = u"ca_wyamz"
    tz = u"Canada/Saskatchewan"
    source = u"http://www.wyamz.ca"
    st_data = (
        u"http://www.wyamz.ca/airQuality/airPointer.php?AP=MeadowLake",
        u"http://www.wyamz.ca/airQuality/airPointer.php?AP=Unity",
        u"http://www.wyamz.ca/airQuality/airPointer.php?AP=Maidstone",
        u"http://www.wyamz.ca/airQuality/airPointer.php?AP=Kindersley",
        u"http://www.wyamz.ca/airQuality/airPointer.php?AP=LloydminsterWest",
        u"http://www.wyamz.ca/airQuality/airPointer.php?AP=LloydminsterEast",
    )
    start_urls = st_data

    def check_date(self, resp):
        row_date = resp.xpath(u'//*[@id="apTable"]/table/tr[2]/td[1]')
        dt_date = row_date.xpath(u"span/text()").extract_first()
        dt_hour = row_date.xpath(u"text()").extract_first()
        dt = str(dt_date) + str(dt_hour)
        new_dt = parse(dt)
        new_dt = new_dt.replace(tzinfo=timezone(self.tz))
        return new_dt

    def get_st_data(self, resp):
        regex = u"AP=(.+)"
        st_id = findall(regex, resp.url)
        st_id = str(st_id[0])

        row_names = resp.xpath(u'//*[@id="apTable"]/table/tr[1]/th/span')
        row_data = resp.xpath(u'//*[@id="apTable"]/table/tr[2]/td')

        new_dt = self.check_date(resp)
        print(new_dt)

        names = list()
        for name in row_names:
            _name = name.xpath(u"text()").extract()
            try:
                names.append(_name[0])

            except IndexError:
                names.append(None)
            # print(_name)

        vals = list()
        for val in row_data:
            _val = val.xpath(u"text()").extract()
            try:
                vals.append(_val[0])
            except IndexError:
                vals.append(None)

        data = zip(names, vals)
        data.pop(0)

        st_data = dict()
        for val in data:
            _name = val[0]
            _val = val[1]
            _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
            if _tmp_dict:
                st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        if st_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = new_dt
            items[u"data_value"] = st_data
            items[u"source"] = self.source
            items[u"source_id"] = st_id

            yield items

    def parse(self, response):
        for st in self.get_st_data(response):
            yield st
