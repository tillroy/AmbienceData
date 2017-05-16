# -*- coding: utf-8 -*-

import sqlite3

from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_

from db_models import db_connect, Map, Station, StationData, WeatherStation, WeatherData, CurrentWeatherData, Data24hr
from settings import SQLITE_STATION_DATA_PATH

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class StationsData(object):
    """SQLite database for station data, their location etc."""

    def __init__(self):
        self.conn = sqlite3.connect(SQLITE_STATION_DATA_PATH)
        self.cursor = self.conn.cursor()

    def get_station(self, code, source):
        # lon, lat, address, source_id, st_name, source
        self.cursor.execute("""
            SELECT source, code, name, address, country, spider, type, lon, lat
            FROM station WHERE code="{0}" AND source="{1}";
            """.format(code, source))

        res = self.cursor.fetchone()
        col_names = ("source", "code", "name", "address", "country", "spider", "type", "lon", "lat")
        res = dict(zip(col_names, res)) if res is not None else None
        # self.conn.close()
        return res

    def close(self):
        self.conn.close()


class DataPipeline(object):
    def open_spider(self, spider):
        self.station = StationsData()

        self.anbiencedataSession = sessionmaker(bind=db_connect())
        # scraper data base session
        SCRAPER_DATABASE = {
            'drivername': 'postgres',
            'host': 'localhost',
            'port': '5432',
            'username': 'postgres',
            'password': 'postgres',
            'database': 'ambiencedata'
        }
        self.scraperSession = sessionmaker(bind=db_connect(SCRAPER_DATABASE))
        self.scraperSession = sessionmaker(bind=db_connect(SCRAPER_DATABASE))

        self.amb_session = self.anbiencedataSession()
        self.scraper_session = self.scraperSession()

    def close_spider(self, spider):
        self.station.close()

        self.amb_session.close()
        self.scraper_session.close()

    def insert_all_daily_data(self, item):
        if item.get("source_if") is not None and item.get("source") is not None:
            station_data = self.station.get_station(item.get("source_if"), item.get("source"))
        else:
            station_data = None

        map_st = Data24hr()


        data_value = item.get("data_value")
        if data_value is not None:

            # station meta data
            if station_data is not None:
                source = station_data.get("source")
                if source is not None:
                    map_st.source = source

                source_id = station_data.get("source_id")
                if source_id is not None:
                    map_st.source_id = source_id

                station_name = station_data.get("station_name")
                if station_name is not None:
                    map_st.station_name = station_name

                address = station_data.get("address")
                if address is not None:
                    map_st.address = address

                country = station_data.get("country")
                if country is not None:
                    map_st.country = country

                spider_name = station_data.get("spider_name")
                if spider_name is not None:
                    map_st.spider_name = spider_name

                spider_type = station_data.get("spider_type")
                if spider_type is not None:
                    map_st.spider_type = spider_type

                lon = station_data.get("lon")
                if lon is not None:
                    map_st.lon = lon

                lat = station_data.get("lat")
                if lat is not None:
                    map_st.lat = lat

            # time data
            data_time = item.get("data_time")
            if data_time is not None:
                map_st.data_time = data_time

            scrap_time = item.get("scrap_time")
            if scrap_time is not None:
                map_st.scrap_time = scrap_time

            # pollution data
            no2 = item.get("no2")
            if no2 is not None:
                map_st.no2 = no2

            so2 = item.get("so2")
            if so2 is not None:
                map_st.so2 = so2

            pm25 = item.get("pm25")
            if pm25 is not None:
                map_st.pm25 = pm25

            pm10 = item.get("pm10")
            if pm10 is not None:
                map_st.pm10 = pm10

            co = item.get("co")
            if co is not None:
                map_st.co = co

            # weather
            temperature = item.get("temperature")
            if temperature is not None:
                map_st.temperature = temperature

            pressure = item.get("pressure")
            if pressure is not None:
                map_st.pressure = pressure

            humidity = item.get("humidity")
            if humidity is not None:
                map_st.humidity = humidity

            ws = item.get("ws")
            if ws is not None:
                map_st.ws = ws

            wd = item.get("wd")
            if wd is not None:
                map_st.wd = wd

        return map_st

    def insert_archive_data(self, item):
        # get station_id
        station = self.amb_session.query(Station).filter(and_(Station.source_id == item['source_id'], Station.source == item['source'])).one()

        station_data = StationData()
        station_data.st_id = station.id
        station_data.data_value = item['data_value']
        station_data.data_time = item['data_time']
        station_data.scrap_time = item['scrap_time']

        return station_data

    def proces_items(self, item, spider):
        try:
            # INFO insert into STATION ARCHIVE

            self.amb_session.add(self.insert_archive_data(item))
            self.amb_session.commit()

            # INFO insert data to the daily table
            self.scraper_session.add(self.insert_all_daily_data(item))
            self.scraper_session.commit()

        except:
            self.amb_session.rollback()
            self.scraper_session.rollback()
            raise

        return item


class MainPipeline:
    def __init__(self):
        # ambiencedata data vase session
        self.anbiencedataSession = sessionmaker(bind=db_connect())
        # scraper data base session
        SCRAPER_DATABASE = {
            'drivername': 'postgres',
            'host': 'localhost',
            'port': '5432',
            'username': 'postgres',
            'password': 'postgres',
            'database': 'ambiencedata'
        }
        self.scraperSession = sessionmaker(bind=db_connect(SCRAPER_DATABASE))
        self.scraperSession = sessionmaker(bind=db_connect(SCRAPER_DATABASE))

    def process_item(self, item, spider):
        amb_session = self.anbiencedataSession()
        scraper_session = self.scraperSession()

        try:
            _source = item['source']
            _source_id = item['source_id']

            # INFO insert data to the STATION table
            station = amb_session.query(Station).filter(and_(Station.source_id == _source_id, Station.source == _source)).one()
            station.data_value = item['data_value']
            station.data_time = item['data_time']
            station.scrap_time = item['scrap_time']

            # INFO insert into STATION ARCHIVE
            station = amb_session.query(Station).filter(and_(Station.source_id == _source_id, Station.source == _source)).one()
            station_data = StationData()
            station_data.st_id = station.id
            station_data.data_value = item['data_value']
            station_data.data_time = item['data_time']
            station_data.scrap_time = item['scrap_time']

            amb_session.add(station_data)

            # INFO insert data to the MAP table
            # FIXME
            map_st = scraper_session.query(Data24hr).filter(and_(Map.source_id == _source_id, Map.source == _source)).one()
            try:
                map_st.data_time = item['data_time']
            except KeyError:
                pass

            try:
                map_st.scrap_time = item['scrap_time']
            except KeyError:
                pass

            try:
                map_st.no2 = item['data_value']['no2']
            except KeyError:
                pass

            try:
                map_st.so2 = item['data_value']['so2']
            except KeyError:
                pass

            try:
                map_st.pm25 = item['data_value']['pm25']
            except KeyError:
                pass

            try:
                map_st.pm10 = item['data_value']['pm10']
            except KeyError:
                pass

            try:
                map_st.co = item['data_value']['co']
            except KeyError:
                pass

            # weather
            try:
                map_st.temp = item['data_value']['temp']
            except KeyError:
                pass

            try:
                map_st.ws = item['data_value']['ws']
            except KeyError:
                pass

            try:
                map_st.wd = item['data_value']['wd']
            except KeyError:
                pass

            try:
                map_st.pressure = item['data_value']['pres']
            except KeyError:
                pass

            try:
                map_st.humidity = item['data_value']['hum']
            except KeyError:
                pass

            amb_session.commit()

        except:
            amb_session.rollback()
            raise
        finally:
            amb_session.close()

        return item


class WeatherPipeline:
    def __init__(self):
        # pass
        engine = db_connect()
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        session = self.Session()

        try:
            # _source = item['source']
            # _source_id = item['source_id']

            # INFO insert into STATION ARCHIVE
            # station = session.query(WeatherStation).filter(and_(WeatherStation.source_id == _source_id, WeatherStation.source == _source)).one()

            station_data = WeatherData()
            station_data.st_id = item['source_id']
            station_data.source = item['source']

            station_data.data = item['data_value']
            station_data.data_time = item['data_time']
            station_data.scrap_time = item['scrap_time']

            session.add(station_data)

            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item


class WeatherForecastPipeline:
    def __init__(self):
        # pass
        engine = db_connect()
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        session = self.Session()

        try:
            # INFO insert into STATION ARCHIVE
            # station = session.query(WeatherStation).filter(and_(WeatherStation.source_id == _source_id, WeatherStation.source == _source)).one()
            station_data = WeatherData()

            station_data.st_id = item['source_id']
            station_data.source = item['source']
            station_data.data_time = item['data_time']
            station_data.scrap_time = item['scrap_time']

            if item.get("forecast_data") is not None:
                station_data.forecast_data = item['forecast_data']

            if item.get("data_value") is not None:
                station_data.data = item['data_value']

            session.add(station_data)

            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item


class WeatherCurrentPipeline:
    def __init__(self):
        # pass
        engine = db_connect()
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        session = self.Session()

        try:
            station_data = WeatherData()

            station_data.st_id = item['source_id']
            station_data.source = item['source']
            station_data.data_time = item['data_time']
            station_data.scrap_time = item['scrap_time']

            if item.get("forecast_data") is not None:
                station_data.forecast_data = item['forecast_data']

            if item.get("data_value") is not None:
                station_data.data = item['data_value']


                # INFO insert data to the CURRENT WEATHER DATA table
                curr_wd = session.query(CurrentWeatherData).filter(
                    and_(
                        CurrentWeatherData.source_id == item['source_id'],
                        CurrentWeatherData.source == item['source'])
                ).one()
                try:
                    curr_wd.data_time = item['data_time']
                except KeyError:
                    pass

                try:
                    curr_wd.scrap_time = item['scrap_time']
                except KeyError:
                    pass

                try:
                    curr_wd.temp = item['data_value']['temp']
                except KeyError:
                    pass

                try:
                    curr_wd.ws = item['data_value']['ws']
                except KeyError:
                    pass

                try:
                    curr_wd.wd = item['data_value']['wd']
                except KeyError:
                    pass

                try:
                    curr_wd.pres = item['data_value']['pres']
                except KeyError:
                    pass

                try:
                    curr_wd.hum = item['data_value']['hum']
                except KeyError:
                    pass

            session.add(station_data)

            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
