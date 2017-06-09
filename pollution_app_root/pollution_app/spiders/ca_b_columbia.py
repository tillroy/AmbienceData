#  coding: utf-8
import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

import scrapy
from pollution_app.pollution import Kind


class BcSpider(scrapy.Spider):
    #  british columbia
    name = u"ca_b_columbia"
    tz = u"EST"
    source = u"http://www.bcairquality.ca"
    start_urls = (u"http://www.bcairquality.ca/aqo/xml/Current_Hour.xml",)

    @staticmethod
    def get_st_pos(resp):
        stations = resp.xpath("STATIONS/STRD")
        data = []
        for station in stations:
            st_info = {
                "source": "http://www.bcairquality.ca",
            }

            st_id = station.xpath("@ID").extract_first()
            if st_id:
                st_info["source_id"] = str(st_id)
            else:
                st_info["source_id"] = None

            lat = station.xpath("@LAT").extract_first()
            if lat:
                lat = str(lat).split(" ")
                lat = int(lat[0]) + (float(lat[1])/60) + (float(lat[2])/3600)
                st_info["lat"] = str(lat)
            else:
                st_info["lat"] = None

            lon = station.xpath("@LONG").extract_first()
            if lon:
                lon = str(lon).split(" ")
                lon = int(lon[0]) + (float(lon[1])/60) + (float(lon[2])/3600)
                st_info["lon"] = str(lon*-1)
            else:
                st_info["lon"] = None

            addr = station.xpath("@ADDR").extract_first()
            if addr:
                st_info["address"] = str(addr)
            else:
                st_info["address"] = "-"

            st_name = station.xpath("@NAME").extract_first()
            if st_name:
                st_info["st_name"] = str(st_name)
            else:
                st_info["st_name"] = "-"

            data.append(st_info)

        return data

    def make_pos_sql_file(self, resp):
        """
        make an sql file for inserting data to the table
        """
        #  source_id  поле яке ідентифікує точку на карті із значенням із сайту
        st_list = self.get_st_pos(resp)
        for st_info in st_list:
            if (st_info["lon"] is not None or st_info["lat"] is not None) and st_info["source_id"] is not None:
                open("add_map_ca_b_columbia.sql", "a").write("""
                INSERT INTO scrapper_map(lon, lat, address, source_id, source, st_name)
                VALUES({0},{1},{2},{3},{4},{5});
                """.format(
                    st_info["lon"],
                    st_info["lat"],
                    """ + str(st_info["address"]) + """,
                    """ + str(st_info["source_id"]) + """,
                    """ + str(st_info["source"]) + """,
                    """ + str(st_info["st_name"]) + """
                    )
                )

    def get_st_data(self, resp):
        stations = resp.xpath(u"STATIONS/STRD")

        for station in stations:
            st_id = station.xpath(u"@ID").extract_first()
            st_id = str(st_id)
            row_st_data = station.xpath(u"READS/RD[1]")

            dt = row_st_data.xpath(u"@DT").extract()
            if dt:
                dt_arr = str(dt[0]).split(u",")
                dt_str = dt_arr[1] + u"." + dt_arr[2] + u"." + dt_arr[0] + u" " + dt_arr[3] + u":" + dt_arr[4] + u":" + dt_arr[5]
                new_dt = parse(str(dt_str))
                # open("testtt.txt", "a").write(str(dt_str) + "\n")
                # new_dt = time.strptime(str(dt[0]), "%Y,%m,%d,%H,%M,%S")
                # new_dt = parse(new_dt)
                new_dt = new_dt.replace(tzinfo=timezone(self.tz))
            else:
                new_dt = None

            row_params = row_st_data.xpath(u"PARAMS/PV")
            st_data = {}
            for pv in row_params:
                _name = pv.xpath(u"@NM").extract_first()

                #  recognize key val from table
                _val = pv.xpath(u"@VL").extract_first()

                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
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
         # self.make_pos_sql_file(response)
        for item in self.get_st_data(response):
            yield item
