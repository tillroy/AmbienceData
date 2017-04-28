# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from db_models import db_connect, Map, Station, StationData, WeatherStation, WeatherData, CurrentWeatherData

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class MainPipeline:
    def __init__(self):
        # pass
        engine = db_connect()
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        session = self.Session()

        try:
            _source = item['source']
            _source_id = item['source_id']

            # INFO insert data to the STATION table
            station = session.query(Station).filter(and_(Station.source_id == _source_id, Station.source == _source)).one()
            station.data_value = item['data_value']
            station.data_time = item['data_time']
            station.scrap_time = item['scrap_time']

            # INFO insert into STATION ARCHIVE
            station = session.query(Station).filter(and_(Station.source_id == _source_id, Station.source == _source)).one()
            station_data = StationData()
            station_data.st_id = station.id
            station_data.data_value = item['data_value']
            station_data.data_time = item['data_time']
            station_data.scrap_time = item['scrap_time']

            session.add(station_data)

            # INFO insert data to the MAP table

            map_st = session.query(Map).filter(and_(Map.source_id == _source_id, Map.source == _source)).one()
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

            session.commit()

        except:
            session.rollback()
            raise
        finally:
            session.close()

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
