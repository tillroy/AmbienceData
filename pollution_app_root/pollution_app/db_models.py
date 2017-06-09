# coding: utf-8
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, create_engine
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import ForeignKey
import settings


Base = declarative_base()


def db_connect(db_set=None):
    if db_set is None:
        db_set = settings.DATABASE

    return create_engine(URL(**db_set))


class Station(Base):
    __tablename__ = 'scrapper_station'

    id = Column(Integer, primary_key=True)
    lon = Column(Float)
    lat = Column(Float)
    st_name = Column(String(250))
    source = Column(String(250))
    source_id = Column(String(250))
    data_time = Column(DateTime(timezone=True))
    scrap_time = Column(DateTime(timezone=True))
    data_value = Column(JSON)


class Data24hr(Base):
    __tablename__ = 'scraper_data_24hr'

    id = Column(Integer, primary_key=True)
    source = Column(String(250))
    source_id = Column(String(250))
    station_name = Column(String(250))
    address = Column(String(250))
    country = Column(String(3))
    spider_name = Column(String(3))
    spider_type = Column(String(3))

    lon = Column(Float)
    lat = Column(Float)

    data_time = Column(DateTime(timezone=True))
    scrap_time = Column(DateTime(timezone=True))

    no2 = Column(Float)
    so2 = Column(Float)
    pm25 = Column(Float)
    pm10 = Column(Float)
    co = Column(Float)

    temperature = Column(Float)
    pressure = Column(Float)
    humidity = Column(Float)
    wd = Column(Float)
    ws = Column(Float)


class Map(Base):
    """Deprecated"""
    __tablename__ = 'scrapper_map'

    id = Column(Integer, primary_key=True)
    data_time = Column(DateTime(timezone=True))
    scrap_time = Column(DateTime(timezone=True))

    lon = Column(Float)
    lat = Column(Float)

    source = Column(String(250))
    source_id = Column(String(250))

    no2 = Column(Float)
    so2 = Column(Float)
    pm25 = Column(Float)
    pm10 = Column(Float)
    co = Column(Float)

    temp = Column(Float)
    pressure = Column(Float)
    wd = Column(Float)
    ws = Column(Float)
    humidity = Column(Float)


class StationData(Base):
    __tablename__ = 'scrapper_station_data'

    data_id = Column(Integer, primary_key=True)
    st_id = Column(Integer)
    data_time = Column(DateTime(timezone=True))
    scrap_time = Column(DateTime(timezone=True))
    data_value = Column(JSON)
    source = Column(String(250))
    source_id = Column(String(250))


# for weather data


class WeatherStation(Base):
    __tablename__ = 'scraper_weather_station'

    id = Column(Integer, primary_key=True)
    lon = Column(Float)
    lat = Column(Float)
    st_name = Column(String(250))
    source = Column(String(250))
    source_id = Column(String(250))


class WeatherData(Base):
    __tablename__ = 'scraper_weather_data'

    id = Column(Integer, primary_key=True)
    st_id = Column(String(250))
    data_time = Column(DateTime(timezone=True))
    scrap_time = Column(DateTime(timezone=True))
    data = Column(JSON)
    forecast_data = Column(JSON)
    source = Column(String(250))


class CurrentWeatherData(Base):
    __tablename__ = 'scraper_current_weather_data'

    id = Column(Integer, primary_key=True)
    source_id = Column(String(250))
    source = Column(String(250))
    data_time = Column(DateTime(timezone=True))
    scrap_time = Column(DateTime(timezone=True))

    temp = Column(Float)
    pres = Column(Float)
    wd = Column(Float)
    ws = Column(Float)
    hum = Column(Float)
