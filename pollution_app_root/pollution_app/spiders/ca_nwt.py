# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall
from scrapy import Spider


class NwtSpider(Spider):
    name = u"ca_nwt"
    tz = u"Canada/Mountain"
    source = u"http://aqm.enr.gov.nt.ca"
    st_pos = (
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=30",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=5",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=2",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=1",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=3",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=14",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=15",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=16",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=17",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=18",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=9",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=8",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=34",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=20",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=21",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=22",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=23",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=28",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=24",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=29",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=12",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=11",
        u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=13",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=27",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=31",
        # u"http://aqm.enr.gov.nt.ca/StationDetails.aspx?ST_ID=32"
    )
    st_data = (
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=30",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=5",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=2",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=1",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=14",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=15",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=16",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=17",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=18",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=9",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=8",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=34",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=20",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=21",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=22",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=23",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=28",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=24",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=29",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=12",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=11",
        u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=13",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=27",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=31",
        # u"http://aqm.enr.gov.nt.ca/StationInfo3.aspx?ST_ID=32"
    )

    start_urls = st_data

    @staticmethod
    def get_st_pos(resp):
        regex = u".*ST_ID=(.+)"
        st_id = findall(regex, resp.url)
        st_id = st_id[0]

        name = resp.xpath(u'//*[@id="form1"]//table')
        rows = name[1].xpath(u"tr")
        st_info = {
            u"source": u"http://aqm.enr.gov.nt.ca",
            u"source_id": str(st_id)
        }
        for row in rows:
            name = row.xpath(u"td[1]/span/text()").extract_first()

            if name == u"Longitude":
                lon = row.xpath(u"td[2]/span/text()").extract_first()
                st_info[u"lon"] = lon

            if name == u"Latitude":
                lat = row.xpath(u"td[2]/span/text()").extract_first()
                st_info[u"lat"] = lat

            if name == u"Location":
                loc = row.xpath(u"td[2]/span/text()").extract_first()
                if loc != u"N/A":
                    st_info[u"address"] = loc
                else:
                    st_info[u"address"] = u"-"

            if name == u"Station Name":
                st_name = row.xpath(u"td[2]/span/text()").extract_first()
                if st_name != u"N/A":
                    st_info[u"st_name"] = st_name
                else:
                    st_info[u"st_name"] = u"-"

        yield st_info

    def make_pos_sql_file(self, resp):
        for st_info in self.get_st_pos(resp):
            if (st_info[u"lon"] is not None or st_info[u"lat"] is not None) and st_info[u"source_id"] is not None:
                open(u"add_map_ca_nwt.sql", u"a").write("""
                INSERT INTO scrapper_map(lon, lat, address, source_id, source, st_name)
                VALUES({0},{1},{2},{3},{4},{5});
                """.format(
                    st_info["lon"],
                    st_info["lat"],
                    """ + str(st_info["address"]) + """,
                    """ + str(st_info["source_id"]) + """,
                    """ + str(st_info["source"]) + """,
                    """ + str(st_info["st_name"]) + """)
                )

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

        table_name = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tbody/tr[1]/td/span')
        # table_name = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]')
        # print("BODY:", resp.text)
        # //*[@id="EnvitechGrid1_GridTable"]
        # //*[@id="EnvitechGrid1_GridTable"]/tbody/tr[2]
        table_val = resp.xpath(u'//*[@id="EnvitechGrid1_GridTable"]/tbody/tr[2]/td/span')

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

        try:
            new_dt = self.check_date(values[0])
        except IndexError:
            new_dt = None

        data = zip(names, values)
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
        # self.make_pos_sql_file(response)
        for st in self.get_st_data(response):
            yield st
