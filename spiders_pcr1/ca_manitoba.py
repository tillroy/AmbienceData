# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class ManitobaSpider(Spider):
    name = u"ca_manitoba"
    tz = u"Canada/Mountain"
    source = u"http://www.gov.mb.ca"
    start_urls = (
        u"http://web31.gov.mb.ca/EnvistaWeb/StationInfo3.aspx?ST_ID=5",
        u"http://web31.gov.mb.ca/EnvistaWeb/StationInfo3.aspx?ST_ID=14",
        u"http://web31.gov.mb.ca/EnvistaWeb/StationInfo3.aspx?ST_ID=6",
        u"http://web31.gov.mb.ca/EnvistaWeb/StationInfo3.aspx?ST_ID=2",
        u"http://web31.gov.mb.ca/EnvistaWeb/StationInfo3.aspx?ST_ID=1"
        )
    # tmp_set = set()

    def check_date(self, smt):
        date = str(smt)
        date_parts = date.split(u" ")
        hour = date_parts[1].split(u":")
        if hour[0] == u"24":
            hour[0] = u"00"
            hour = u":".join(hour)
            date_parts[1] = hour

        date = u" ".join(date_parts)
        # print(date)
        date = parse(date)
        date = date.replace(tzinfo=timezone(self.tz))
        return date

    def get_st_data(self, resp):
        regex = u".*ST_ID=(.+)"
        st_id = findall(regex, resp.url)
        st_id = str(st_id[0])

        table_name = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[1]/td/span')
        table_val = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tr[2]/td/span')
        names = list()
        for row in table_name:
            # print(row)
            row_name = row.xpath(u"@title").extract_first()
            regex_name = u"(.+)"
            _name = findall(regex_name, row_name)
            try:
                names.append(_name[0])
            except IndexError:
                names.append(None)

        values = list()
        for row in table_val:
            row_val = row.xpath(u"@title").extract_first()
            regex_val = u"\) (.+)"
            _val = findall(regex_val, row_val)
            try:
                values.append(_val[0])
            except IndexError:
                values.append(None)

        # for n in names:
        #     self.tmp_set.add(n)
        #     open("manitoba_names.txt", "a").write(str(self.tmp_set) + "\n")

        try:
            new_dt = self.check_date(values[0])
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
        for st in self.get_st_data(response):
            yield st
