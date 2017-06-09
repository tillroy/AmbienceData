# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


def get_str_date():
    tz = u"Canada/Mountain"
    # потррібно для динамічної підстановки в URL для кравлінга
    place_time = datetime.now(tz=timezone(tz))
    year = str(place_time.year)
    month = str(place_time.month)
    if len(month) == 1:
        month = u"0" + month

    day = str(place_time.day)
    if len(day) == 1:
        day = u"0" + day

    hour = int(place_time.hour)-1
    # hour = str(place_time.hour)
    hour = str(hour)
    if len(hour) == 1:
        hour = u"0" + hour

    date = year + month + day
    return {u"date": date, u"hour": hour}


class AlbertaSpider(Spider):
    name = u"ca_alberta"
    tz = u"Canada/Mountain"
    source = u"http://esrd.alberta.ca"

    href = u"https://data.environment.alberta.ca/Services/AirQualityV2/AQHI.svc/StationMeasurements?$format=xml&$select=Value,ReadingDate,ParameterName,StationName,Parameter/UnitCode,Parameter/Decimals,Parameter/Objective&$expand=Parameter&$filter=Date/Id%20eq%20{0}%20and%20Time/HourOfDay24%20eq%20{1}&$orderby=StationName,ParameterName"
    # 20160315
    # 02
    # потррібно для динамічної підстановки в URL для кравлінга
    param = get_str_date()
    href = href.format(param[u"date"], param[u"hour"])
    # href = href.format(param["date"], "08")
    st_info = (
        href,
    )
    start_urls = st_info

    def get_date(self, date_str):
        dt = parse(date_str)
        new_dt = dt.replace(tzinfo=timezone(self.tz))
        return new_dt

    def read_st_data(self, resp):
        st_data = resp.xpath(u"StationMeasurement")
        stations = dict()
        dt = resp.xpath(u"StationMeasurement[1]/ReadingDate/text()").extract_first()
        new_dt = self.get_date(dt)
        print(new_dt)

        for data in st_data:
            name = data.xpath(u"StationName/text()").extract_first()
            poll_name = data.xpath(u"ParameterName/text()").extract_first()
            poll_val = data.xpath(u"Value/text()").extract_first()
            _name = str(poll_name)
            _val = str(poll_val)
            # print(_name, _val)
            if name not in stations:
                name = str(name)
                stations[name] = dict()

            _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
            # print(_tmp_dict)
            if _tmp_dict:
                stations[name][_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        result = (stations, new_dt)
        return result

    def get_st_data(self, resp):
        data = self.read_st_data(resp)
        st_data = data[0]
        new_dt = data[1]
        # print(st_data)
        for st_id in st_data:
            print(st_id)
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = new_dt
            items[u"data_value"] = st_data[st_id]
            items[u"source"] = self.source
            items[u"source_id"] = st_id
            items[u"st_name"] = st_id

            yield items

    def parse(self, response):
        for st in self.get_st_data(response):
            yield st

        # self.read_st_data(response)