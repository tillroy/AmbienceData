# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
import numpy as np
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class TexasSpider(Spider):
    name = u"us_texas_pollution"
    source = u"https://www.tceq.texas.gov"
    tz = u"US/Central"

    def start_requests(self):
        # location data
        # codes = (
        #     u"0", u"884", u"1069", u"1075", u"762", u"1003", u"1238", u"823", u"13", u"1050", u"155", u"748",
        #     u"25", u"821", u"1054", u"29", u"24", u"125", u"1049", u"827", u"93", u"1015", u"1233", u"1234",
        #     u"1070", u"221", u"219", u"818", u"847", u"825", u"229", u"231", u"820", u"1034", u"955", u"1040",
        #     u"737", u"736", u"738", u"243", u"134", u"819", u"88", u"831", u"241", u"240", u"82", u"109", u"225",
        #     u"1027", u"896", u"746", u"73", u"198", u"1239", u"67", u"64", u"1005", u"68", u"69", u"989", u"880",
        #     u"10", u"828", u"1112", u"1071", u"193", u"1053", u"188", u"187", u"756", u"1022", u"903", u"824",
        #     u"183", u"1010", u"63", u"1", u"46", u"751", u"192", u"1120", u"161", u"154", u"165", u"812", u"1030",
        #     u"815", u"743", u"1073", u"1025", u"1067", u"1122", u"1046", u"45", u"228", u"1237", u"1033", u"213",
        #     u"212", u"210", u"207", u"1019", u"1068", u"126", u"915", u"747", u"867", u"863", u"862", u"1065",
        #     u"878", u"947", u"1113", u"96", u"34", u"3", u"91", u"886", u"1240", u"85", u"1056", u"43", u"842",
        #     u"901", u"137", u"127", u"129", u"107", u"110", u"1271", u"123", u"121", u"1121", u"131", u"1052",
        #     u"15", u"119", u"114", u"1032", u"882", u"1021", u"949", u"873", u"953", u"167", u"927", u"89",
        #     u"1026", u"99", u"1119", u"839", u"81", u"40", u"1043", u"956", u"104", u"866", u"836", u"307",
        #     u"1064", u"130", u"18", u"19", u"1114", u"138", u"1264", u"897", u"4", u"742", u"1051", u"223",
        #     u"928", u"837", u"749", u"174", u"116", u"1066", u"97", u"204", u"5", u"921", u"817", u"822", u"1031",
        #     u"133", u"11", u"180", u"1062", u"922", u"1244", u"1272", u"923", u"256", u"257", u"799", u"904",
        #     u"61", u"113", u"960", u"912", u"1232", u"86", u"92", u"90", u"1057", u"1242", u"164", u"1236", u"58",
        #     u"894", u"1241", u"1055", u"239", u"235", u"103", u"826", u"230", u"745", u"9", u"798", u"740", u"810",
        #     u"122", u"151", u"215", u"153", u"761", u"1267", u"12", u"1074", u"1000", u"1265", u"758", u"146",
        #     u"1006", u"1008", u"1007", u"145", u"838", u"879", u"877", u"53", u"1012", u"1011", u"1268", u"987",
        #     u"1014", u"1013", u"967", u"22", u"902", u"895", u"937", u"759", u"1072", u"834", u"62", u"1076",
        #     u"875", u"159", u"7", u"985"
        # )
        # href = u"http://www17.tceq.texas.gov/tamis/index.cfm?fuseaction=report.view_site&siteID={station_id}&siteOrderBy=name&showActiveOnly=1&showActMonOnly=1&formSub=1&tab=info"
        #
        # header = u";".join((u"station_code", u"station_name", u"address", u"lon", u"lat", u"elev", u"status", u"\n"))
        #
        # f = open(u"us_texas_station_coordinates.txt", u"w")
        # f.write(header.encode())
        # f.close()
        #
        # for code_value in codes:
        #     url = href.format(station_id=code_value)
        #
        #     yield Request(
        #         url=url,
        #         callback=self.get_station_location,
        #         meta={u"code": code_value}
        #     )

        # for station data
        codes = (
            u"1", u"2", u"3", u"4", u"8", u"9", u"11", u"12", u"13", u"15", u"17", u"19", u"21", u"23", u"26", u"28",
            u"31", u"35", u"36", u"37", u"38", u"41", u"43", u"44", u"45", u"47", u"49", u"52", u"53", u"56", u"58",
            u"59", u"60", u"61", u"63", u"64", u"66", u"69", u"70", u"71", u"72", u"73", u"75", u"76", u"77", u"78",
            u"79", u"80", u"82", u"83", u"84", u"85", u"87", u"88", u"96", u"98", u"104", u"105", u"114", u"119",
            u"123", u"131", u"136", u"140", u"145", u"148", u"167", u"168", u"169", u"171", u"199", u"243", u"301",
            u"303", u"304", u"310", u"311", u"312", u"313", u"314", u"316", u"319", u"320", u"323", u"326", u"402",
            u"404", u"405", u"406", u"408", u"409", u"410", u"414", u"416", u"501", u"502", u"503", u"504", u"505",
            u"506", u"551", u"552", u"553", u"554", u"556", u"557", u"558", u"559", u"560", u"561", u"562", u"563",
            u"570", u"571", u"572", u"601", u"609", u"614", u"615", u"616", u"617", u"618", u"620", u"621", u"622",
            u"623", u"624", u"625", u"626", u"633", u"640", u"643", u"660", u"664", u"669", u"670", u"671", u"673",
            u"676", u"677", u"678", u"684", u"685", u"690", u"693", u"695", u"696", u"697", u"698", u"699", u"1006",
            u"1007", u"1008", u"1009", u"1010", u"1012", u"1013", u"1014", u"1015", u"1016", u"1017", u"1018", u"1019",
            u"1020", u"1021", u"1022", u"1023", u"1024", u"1025", u"1028", u"1029", u"1031", u"1032", u"1034", u"1035",
            u"1036", u"1037", u"1038", u"1044", u"1045", u"1046", u"1047", u"1049", u"1051", u"1052", u"1053", u"1054",
            u"1062", u"1063", u"1064", u"1065", u"1066", u"1067", u"1068", u"1069", u"1070", u"1071", u"1072", u"1073",
            u"1075", u"1076", u"1077", u"1078", u"1079", u"1080", u"1081", u"1083", u"1500", u"1501", u"1502", u"1503",
            u"1504", u"1505", u"1506", u"1507", u"1508", u"1509", u"1602", u"1603", u"1604", u"1605", u"1606", u"1607",
            u"1628", u"1675", u"5002", u"5003", u"5004", u"5005", u"5006", u"5007", u"5008", u"5009", u"5010", u"5011",
            u"5012", u"5013", u"5014", u"5015", u"5016", u"5017", u"5018", u"6602"
        )
        # codes = (u"104", )

        href = u"https://www.tceq.texas.gov/cgi-bin/compliance/monops/daily_summary.pl?select_date=user&prev_emulation=0&first_look=no&cams={station_id}"

        for code_value in codes:
            url = href.format(station_id=code_value)

            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={u"code": code_value}
            )

    def get_station_location(self, resp):
        raw_data = resp.xpath(u"//*[@id='content']/div[5]/ul")

        try:
            station_name = raw_data.xpath(u"li[2]/text()").re(u"CAMS:\s?(.+)")[0]
            if u" " == station_name:
                station_name = u"NA"
        except IndexError:
            station_name = "NA"

        try:
            status = raw_data.xpath(u"li[4]/text()").re(u"Current Status:\s?(.+)")[0]
        except IndexError:
            status = "NA"

        try:
            address = raw_data.xpath(u"li[8]/text()").re(u"Address: (.+)")[0]
        except IndexError:
            address = "-"

        raw_location = raw_data.xpath(u"li[10]/ul")

        try:
            lon = raw_location.xpath(u"li[2]/text()").re(u"\((-?\d+.\d+).?\)")[0]
        except IndexError:
            lon = "NA"

        try:
            lat = raw_location.xpath(u"li[1]/text()").re(u"\((-?\d+.\d+).?\)")[0]
        except IndexError:
            lat = "NA"

        try:
            elev = raw_location.xpath(u"li[3]/text()").re(u"(-?\d+\.\d+).?")[0]
        except IndexError:
            elev = "NA"

        record = u";".join((resp.meta["code"], station_name, address, lon, lat, elev, status, u"\n"))
        open(u"us_texas_station_coordinates.txt", u"a").write(record.encode())
        # print(record)

    @staticmethod
    def validate_hours(hours):
        mid_ind = hours.index(u"Mid")

        try:
            noon_ind = hours.index(u"Noon")
        except ValueError:
            noon_ind = None

        if noon_ind is None:
            hours[mid_ind] = u"0:00"
            hours = [x.strip(u" ") for x in hours]
            return hours

        elif noon_ind is not None:
            hours[mid_ind] = u"0"
            hours[noon_ind] = u"12"
            hours = [int(x.strip(u" ").replace(u":00", u"")) for x in hours]

            new_hours = list()
            for key, el in enumerate(hours):
                if key > noon_ind:
                    new_hours.append(el + 12)
                else:
                    new_hours.append(el)

            hours = [u"{0}:00".format(x) for x in new_hours]
            return hours

    @staticmethod
    def coerce_float(dig):
        try:
            res = float(dig)
            return res
        except ValueError:
            return np.nan

    def get_station_data(self, resp):
        raw_table = resp.xpath(u"//*[@id='meteostar_wrapper']/div[3]/table/tr")[2:-3]

        raw_data_time = resp.xpath(u"//*[@id='meteostar_wrapper']/form/table[2]/tr[2]/td")
        month = raw_data_time[0].xpath(u"select/option[@selected='selected']/text()").extract_first()
        day = raw_data_time[1].xpath(u"select/option[@selected='selected']/text()").extract_first()
        year = raw_data_time[2].xpath(u"select/option[@selected='selected']/text()").extract_first()

        pollutants_hour = resp.xpath(u"//*[@id='meteostar_wrapper']/div[3]/table/tr[2]/th/text()").extract()
        pollutants_hour = self.validate_hours(pollutants_hour)
        data_times = [parser.parse(u" ".join((month, day, year, hour))) for hour in pollutants_hour]

        units = {
            u"o3": u"ppb",
            u"ws": u"mph",
            u"wd": u"deg",
            u"temp": u"degf",
            u"pm10": u"ug/m3",
            u"pm25": u"ug/m3",
            u"pm": u"ug/m3",
            u"co": u"ppm",
            u"rain": u"in",
            u"no": u"ppb",
            u"no2": u"ppb",
            u"pres": u"mbar",
            u"hum_rel": u"%",
            u"no_y": u"ppb",
            u"so2": u"ppb",
        }

        station_data = dict()
        table = list()
        for row in raw_table:
            try:
                pollutant_name = row.xpath(u"td[1]/a/b/text()").extract()[0]
            except IndexError:
                pollutant_name = None

            pollutants_data = row.xpath(u"td")[1:-2]
            pollutants_data = ["".join(el.xpath(u".//text()").extract()) for el in pollutants_data]
            pollutants_data = map(self.coerce_float, pollutants_data)

            print(pollutants_data)
            print(pollutants_hour)

            records = list()
            for el in zip([pollutant_name] * len(data_times), pollutants_data, data_times):
                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                pollutant.set_raw_name(el[0])
                pollutant.set_raw_value(el[1])
                pollutant.set_raw_units(units.get(pollutant.get_name()))

                res = {
                    "name": pollutant.get_name(),
                    "value": pollutant.get_value(),
                    "date": el[2],
                    "unit": pollutant.get_units()
                }
                records.append(res)

            table.extend(records)

        df = pd.DataFrame(table)
        df["value"] = df["value"].astype(float)
        df = df.dropna(axis=0)
        # print(df)

        current_data_time = df["date"].max()
        current_data = df[df["date"] == current_data_time]

        print(current_data)

        station_data = dict()
        for el in current_data[["name", "value"]].itertuples(index=False):
            station_data[el[0]] = el[1]

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items
