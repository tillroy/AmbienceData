# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class CaliforniaWeatherSpider(Spider):
    name = u"us_california_weather"
    # curl http://localhost:6821/schedule.json -d project=ambiencedata_app -d spider=us_california_weather
    source = u"http://www.cimis.water.ca.gov"
    tz = u"US/Pacific"

    custom_settings = {
        "ITEM_PIPELINES": {'pollution_app.pipelines.WeatherCurrentPipeline': 300}
    }

    def start_requests(self):
        # station_data = u"http://et.water.ca.gov/api/station"
        today = datetime.now().strftime("%Y-%m-%d")

        app_key = u"70d3135c-3715-4a5f-95e8-3f2a8a9caaff"
        start_date = today
        end_date = today

        codes = (
            u"2", u"5", u"6", u"7", u"12", u"13", u"15", u"19", u"21", u"35", u"39", u"41", u"43", u"44", u"47", u"52",
            u"54", u"56", u"57", u"62", u"64", u"68", u"70", u"71", u"75", u"77", u"78", u"80", u"83", u"84", u"86",
            u"87", u"88", u"90", u"91", u"92", u"99", u"103", u"104", u"105", u"106", u"107", u"109", u"111", u"113",
            u"114", u"116", u"117", u"121", u"124", u"125", u"126", u"129", u"131", u"135", u"136", u"139", u"140",
            u"142", u"143", u"144", u"146", u"147", u"148", u"150", u"151", u"152", u"153", u"155", u"157", u"158",
            u"159", u"160", u"161", u"163", u"165", u"167", u"169", u"170", u"171", u"173", u"174", u"175", u"178",
            u"179", u"181", u"182", u"183", u"184", u"187", u"188", u"189", u"190", u"191", u"192", u"193", u"194",
            u"195", u"196", u"197", u"198", u"199", u"200", u"202", u"203", u"204", u"205", u"206", u"207", u"208",
            u"209", u"210", u"211", u"212", u"213", u"214", u"215", u"216", u"217", u"218", u"219", u"220", u"221",
            u"222", u"224", u"225", u"226", u"227", u"228", u"229", u"230", u"231", u"232", u"233", u"234", u"235",
            u"236", u"237", u"238", u"239", u"240", u"241", u"242", u"243", u"244", u"245", u"246", u"247", u"248",
            u"249", u"250", u"251", u"252"
        )
        # codes = (u"2")

        weather_data = u"http://et.water.ca.gov/api/data?appKey={app_key}&targets={target}&startDate={start_date}&endDate={end_date}&dataItems=hly-air-tmp,hly-dew-pnt,hly-eto,hly-net-rad,hly-asce-eto,hly-asce-etr,hly-precip,hly-rel-hum,hly-res-wind,hly-soil-tmp,hly-sol-rad,hly-vap-pres,hly-wind-dir,hly-wind-spd"
        for code_value in codes:
            url = weather_data.format(app_key=app_key, target=code_value, start_date=start_date, end_date=end_date)
            yield Request(
                url=url,
                callback=self.get_station_data,
                meta={u"code": code_value}
            )

    def get_station_location(self, resp):
        stations = resp.xpath(u"//stations/station[./is-active/text()='True']")
        data = list()
        for el in stations:
            station_id = el.xpath(u"./@station-nbr").extract_first()

            county = el.xpath(u"./@county").extract_first()
            city = el.xpath(u"./@city").extract_first()
            address = u", ".join((county, city))

            name = el.xpath(u"./@name").extract_first()
            elev = el.xpath(u"./elevation/text()").extract_first()
            lat = el.xpath(u"./hms-latitude/text()").re(u".+\/\s+(-?\d+\.?\d+)$")[0]
            lon = el.xpath(u"./hms-longitude/text()").re(u".+\/\s+(-?\d+\.?\d+)$")[0]
            rec = {
                u"station_id": station_id,
                u"name": name,
                u"elev": elev,
                u"lat": lat,
                u"lon": lon,
                u"address": address,
            }
            data.append(rec)

        df = pd.DataFrame(data)
        df[[u"station_id", u"name", u"address", u"lon", u"lat", u"elev"]].to_csv(
            "us_california_weather_stations_location.csv",
            sep=";",
            index=False
        )
        # print(df)

    def get_station_data(self, resp):
        records = resp.xpath(u"//record")
        table = list()
        for rec in records:
            date = rec.xpath(u"./@date").extract_first()
            hour = rec.xpath(u"./@hour").extract_first()
            if hour == u"2400":
                hour = u"0000"

            raw_data_date = u" ".join((date, hour))
            data_date = parser.parse(raw_data_date)

            values = rec.xpath(u"child::node()")
            for val in values:
                pollutant_name = val.xpath(u"name(.)").extract_first()
                pollutant_value = val.xpath(u"./text()").extract_first()
                pollutant_unit = val.xpath(u"./@unit").extract_first()
                row = {
                    u"date": data_date,
                    u"pollutant_name": pollutant_name,
                    u"pollutant_value": pollutant_value,
                    u"pollutant_unit": pollutant_unit,

                }
                table.append(row)

        data = pd.DataFrame(table)
        data = data.dropna(axis=0)

        current_data_time = data[u"date"].max()
        curr_data = data[data[u"date"] == current_data_time]

        station_data = dict()
        for el in curr_data[[u"pollutant_name", u"pollutant_value", u"pollutant_unit"]].itertuples(index=False):
            pollutant_name = el[0]
            pollutant_value = el[1]
            pollutant_units = el[2]

            pollutant = Feature(self.name)
            pollutant.set_source(self.source)
            pollutant.set_raw_name(pollutant_name)
            pollutant.set_raw_value(pollutant_value)
            pollutant.set_raw_units(pollutant_units)

            # print("answare", pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
            if pollutant.get_name() is not None and pollutant.get_value() is not None:
                station_data[pollutant.get_name()] = pollutant.get_value()

        if station_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
            items[u"data_value"] = station_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            return items
