# coding: utf-8
import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

import re
import scrapy
from pollution_app.pollution import Kind


class CpcbSpider(scrapy.Spider):
    name = u"in_cpcb"
    tz = u"Asia/Calcutta"
    source = u"http://www.cpcb.gov.in"

    st_pos = (
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Howrah&StateId=29&CityId=548",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Victoria&StateId=29&CityId=300",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=HALDIA&StateId=29&CityId=549",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Alandur &StateId=25&CityId=546",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=IIT&StateId=25&CityId=546",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Manali&StateId=25&CityId=546",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=BTM&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Peenya&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=BWSSB&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=SGHALLI&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=CRS&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Hyderabad&StateId=30&CityId=7",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=ZooPark&StateId=30&CityId=7",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=MPCB Bandra&StateId=16&CityId=310",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=NMMC Airoli&StateId=16&CityId=310",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Karve Road Pune&StateId=16&CityId=312",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Solapur&StateId=16&CityId=314",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Chandrapur&StateId=16&CityId=329",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Maninagar&StateId=8&CityId=337",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Jodhpur&StateId=23&CityId=212",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Jaipur&StateId=23&CityId=223",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Panchkula&StateId=9&CityId=348",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Rohtak&StateId=9&CityId=360",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=HSPCBGurgaon&StateId=9&CityId=364",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Sector16A Faridabad&StateId=9&CityId=365",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=ITO&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=D.C.E.&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Shadipur&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Dwarka&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=IHBAS&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=R K Puram&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Mandir Marg&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Punjabi Bagh&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=IGI Airport&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Civil Lines&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Anand Vihar&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Muzaffarpur Collectorate&StateId=4&CityId=54",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=IGSC Planetarium Complex&StateId=4&CityId=70",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Gaya Collectorate&StateId=4&CityId=75",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Sanjay Palace&StateId=28&CityId=253",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Talkatora&StateId=28&CityId=256",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Lalbagh&StateId=28&CityId=256",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Central School&StateId=28&CityId=256",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Nehru Nagar&StateId=28&CityId=278",
        u"http://www.cpcb.gov.in/CAAQM/frmStationDescription.aspx?StationName=Ardhali Bazar&StateId=28&CityId=270"
    )
    st_data = (
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Howrah&StateId=29&CityId=548",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Victoria&StateId=29&CityId=300",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=HALDIA&StateId=29&CityId=549",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Alandur &StateId=25&CityId=546",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=IIT&StateId=25&CityId=546",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Manali&StateId=25&CityId=546",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=BTM&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Peenya&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=BWSSB&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=SGHALLI&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=CRS&StateId=13&CityId=136",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Hyderabad&StateId=30&CityId=7",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=ZooPark&StateId=30&CityId=7",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=MPCB Bandra&StateId=16&CityId=310",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=NMMC Airoli&StateId=16&CityId=310",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Karve Road Pune&StateId=16&CityId=312",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Solapur&StateId=16&CityId=314",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Chandrapur&StateId=16&CityId=329",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Maninagar&StateId=8&CityId=337",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Jodhpur&StateId=23&CityId=212",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Jaipur&StateId=23&CityId=223",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Panchkula&StateId=9&CityId=348",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Rohtak&StateId=9&CityId=360",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=HSPCBGurgaon&StateId=9&CityId=364",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Sector16A Faridabad&StateId=9&CityId=365",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=ITO&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=D.C.E.&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Shadipur&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Dwarka&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=IHBAS&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=R K Puram&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Mandir Marg&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Punjabi Bagh&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=IGI Airport&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Civil Lines&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Anand Vihar&StateId=6&CityId=85",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Muzaffarpur Collectorate&StateId=4&CityId=54",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=IGSC Planetarium Complex&StateId=4&CityId=70",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Gaya Collectorate&StateId=4&CityId=75",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Sanjay Palace&StateId=28&CityId=253",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Talkatora&StateId=28&CityId=256",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Lalbagh&StateId=28&CityId=256",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Central School&StateId=28&CityId=256",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Nehru Nagar&StateId=28&CityId=278",
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Ardhali Bazar&StateId=28&CityId=270"
    )

    st_data = (
        u"http://www.cpcb.gov.in/CAAQM/frmCurrentDataNew.aspx?StationName=Ardhali Bazar&StateId=28&CityId=270",
    )

    start_urls = st_data


    @staticmethod
    def get_st_pos(resp):
        #  get local id from url
        regex_state = u"StateId=(.+?)&"
        regex_city = u"CityId=(.+)"
        regex_station = u"StationName=(.+?)&"
        _state_id = re.findall(regex_state, resp.url)
        _city_id = re.findall(regex_city, resp.url)
        _station_id = re.findall(regex_station, resp.url)
        _id = u"".join((_station_id[0].replace(u"%20", u" "), _state_id[0], _city_id[0]))

        st_info = {
            u"source": u"http://www.cpcb.gov.in",
        }

        if _id:
            st_info[u"source_id"] = _id
        else:
            st_info[u"source_id"] = None

        lat = resp.xpath(u'//*[@id="lblLatitude"]/text()').extract()
        if len(lat):
            st_info[u"lat"] = str(lat[0])
        else:
            st_info[u"lat"] = None

        lon = resp.xpath(u'//*[@id="lblLongitude"]/text()').extract()
        if len(lon):
            st_info[u"lon"] = str(lon[0])
        else:
            st_info[u"lon"] = None

        st_name = resp.xpath(u'//*[@id="lblStation"]/text()').extract()
        if len(st_name):
            st_info[u"st_name"] = str(st_name[0])
        else:
            st_info[u"st_name"] = u"-"

        state = resp.xpath(u'//*[@id="lblState"]/text()').extract()
        city = resp.xpath(u'//*[@id="lblCity"]/text()').extract()
        if len(state):
            state = str(state[0])
            city = str(city[0])
            st_info[u"address"] = state + u", " + city
        else:
            st_info[u"address"] = u"-"
        # print(st_info)
        return st_info

    def make_pos_sql_file(self, resp):
        """
        make an sql file for inserting data to the table
        """
        #  source_id  поле яке ідентифікує точку на карті із значенням із сайту
        st_info = self.get_st_pos(resp)
        if (st_info["lon"] is not None or st_info["lat"] is not None) and st_info["source_id"] is not None:
            open("add_map_in_cpcb.sql", "a").write("""
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

    def get_st_data(self, resp):
        #  get local id from url
        regex_state = u"StateId=(.+?)&"
        regex_city = u"CityId=(.+)"
        regex_station = u"StationName=(.+?)&"
        _state_id = re.findall(regex_state, resp.url)
        _city_id = re.findall(regex_city, resp.url)
        _station_id = re.findall(regex_station, resp.url)
        st_id = u"".join((_station_id[0].replace(u"%20", u" "), _state_id[0], _city_id[0]))

        table = resp.xpath(u'//*[@id="lblReportCurrentData"]/table/child::*')
        data_date = resp.xpath(u'//*[@id="lblCurrentDateTime"]/text()').extract_first()

        st_data = {}
        for el in table:
            col = el.xpath(u"child::td")
            try:
                name = col[0].xpath(u"text()").extract()
                _name = name[0]
                _val = col[3].xpath(u"span/text()").extract_first()

                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            except IndexError:
                pass

        if data_date:
            data_date = data_date.replace(u"Date Time : ", u"")
            new_dt = parse(data_date)
            new_dt = new_dt.replace(tzinfo=timezone(self.tz))
        else:
            new_dt = None

        if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = new_dt
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = st_id
                return items

    def parse(self, response):
        # self.make_pos_sql_file(response)
        yield self.get_st_data(response)
