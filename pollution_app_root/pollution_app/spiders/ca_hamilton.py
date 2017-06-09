# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class HamiltonSpider(Spider):
    name = u"ca_hamilton"
    source = u"http://www.hamnair.ca"
    st_data = (
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=7",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=28",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=19",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=10",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=27",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=34",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=22",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=16",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=4",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=30",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=1",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=38",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=33",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=35",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=32",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=13",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=25",
        u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=31"
    )
    test_href = (u"http://reporting.hamnair.ca/StationInfo2.aspx?ST_ID=7",)
    start_urls = st_data
    tz = u"EST"
    temp_list = set()

    def check_date(self, smt):
        date = str(smt)
        date_parts = date.split(u" ")
        hour = date_parts[1].split(u":")
        if hour[0] == u"24":
            hour[0] = u"00"
            hour = u":".join(hour)
            date_parts[1] = hour

        date = u" ".join(date_parts)
        date = parse(date)
        date = date.replace(tzinfo=timezone(self.tz))
        return date

    def get_st_data(self, resp):
        regex = u".*ST_ID=(.+)"
        st_id = findall(regex, resp.url)
        st_id = str(st_id[0])
        # print(st_id)

        table_name = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[1]/td/span')
        table_val = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td/span')
        names = []
        for row in table_name:
            # print(row)
            row_name = row.xpath(u"@title").extract_first()
            regex_name = u"(.+)"
            _name = findall(regex_name, row_name)
            try:
                names.append(_name[0])
            except IndexError:
                names.append(None)

        values = []
        for row in table_val:
            row_val = row.xpath(u"@title").extract_first()
            regex_val = u"\) (.+)"
            _val = findall(regex_val, row_val)
            try:
                values.append(_val[0])
            except IndexError:
                values.append(None)

        try:
            new_dt = self.check_date(values[0])
            # print(new_dt)
        except IndexError:
            new_dt = None

        data = zip(names, values)
        data.pop(0)
        # print(data)

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
        # self.get_st_data(response)
        for st in self.get_st_data(response):
            yield st
