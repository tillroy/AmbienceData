# coding: utf-8
from scrapy import Spider, Request
from scrapy.exceptions import CloseSpider

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from pollution_app.pollution import Kind

from w3lib.url import add_or_replace_parameter, url_query_parameter
from datetime import datetime
from pytz import timezone
from pollution_app.rextension import time_to_dict

from pandas import DataFrame, merge


class BuenosairesSpider(Spider):
    name = u"ar_buenosaires"
    tz = u"America/Argentina/Buenos_Aires"
    source = u"http://www.buenosaires.gob.ar"
    date_now = datetime.now()

    def start_requests(self):
        now = time_to_dict(self.date_now)
        href = u"http://www.buenosaires.gob.ar/areas/med_ambiente/apra/calidad_amb/red_monitoreo/index.php?contaminante=1&estacion=1"
        href = add_or_replace_parameter(href, u"fecha_dia", now[u"day"])
        href = add_or_replace_parameter(href, u"fecha_mes", now[u"month"])
        href = add_or_replace_parameter(href, u"fecha_anio", now[u"year"])
        href = add_or_replace_parameter(href, u"menu_id", u"")
        href = add_or_replace_parameter(href, u"buscar", u"Buscar")

        yield Request(
            url=href,
            callback=self.check_validity
        )
        yield Request(
            url=add_or_replace_parameter(href, u"estacion", 2),
            callback=self.check_validity
        )
        yield Request(
            url=add_or_replace_parameter(href, u"estacion", 3),
            callback=self.check_validity
        )

    @staticmethod
    def get_pollutant_name(url):
        code_name = url_query_parameter(url, u"contaminante")
        name = None
        if code_name == u"1":
            name = u"co"
        elif code_name == u"2":
            name = u"no2"
        elif code_name == u"3":
            name = u"pm10"

        return name

    @staticmethod
    def get_station_code(url):
        code = url_query_parameter(url, u"estacion")

        return code

    @staticmethod
    def get_date_from_url(url):
        day = url_query_parameter(url, u"fecha_dia")
        month = url_query_parameter(url, u"fecha_mes")
        year = url_query_parameter(url, u"fecha_anio")

        page_date = {
            u"day": int(day),
            u"month": int(month),
            u"year": int(year),
        }

        return page_date

    @staticmethod
    def get_date_from_body(body):
        day = body.css(u"select#fecha_dia > option[selected] ::attr(value)").extract_first()
        month = body.css(u"select#fecha_mes > option[selected] ::attr(value)").extract_first()
        year = body.css(u"select#fecha_anio > option[selected] ::attr(value)").extract_first()

        page_date = {
            u"day": int(day),
            u"month": int(month),
            u"year": int(year),
        }

        return page_date

    def is_equal_dates(self, resp):
        """compare dates from URL string and from body"""
        date_page = self.get_date_from_body(resp)
        date_url = self.get_date_from_url(resp.url)

        equal = set(date_page.items()) & set(date_url.items())
        if len(equal) == 3:
            return True
        else:
            return False

    def url_to_datetime(self, resp):
        date_url = self.get_date_from_url(resp.url)
        _date = datetime(
            day=int(date_url[u"day"]),
            month=int(date_url[u"month"]),
            year=int(date_url[u"year"])
        )

        return _date

    def new_request(self, resp):
        date_url = self.url_to_datetime(resp)

        # decrease day
        previous = time_to_dict(date_url, 1)

        new_url = add_or_replace_parameter(resp.url, u"fecha_dia", previous[u"day"])
        new_url = add_or_replace_parameter(new_url, u"fecha_mes", previous[u"month"])
        new_url = add_or_replace_parameter(new_url, u"fecha_anio", previous[u"year"])
        return Request(
                url=new_url,
                callback=self.check_validity,
                # dont_filter=True
            )

    def check_validity(self, resp):
        """check response and make new request if it's bad"""
        date_url = self.url_to_datetime(resp)
        diff = (self.date_now - date_url).days
        if diff == 10:
            raise CloseSpider(u"Too big difference between dates |{0} day(s)|".format(diff))
        elif self.is_equal_dates(resp):
            table = resp.xpath(u'//*[@id="grafico"]/table/tr[1]/td[2]/img')
            # print(len(table))
            if not table:
                print(u"NEW request")
                yield self.new_request(resp)
            else:
                print(u"GOOODDDDD URL!!!!!!!!!!!!!!!STOP...")
                yield Request(
                    url=resp.url,
                    callback=self.parse,
                    dont_filter=True
                )
        else:
            print(u"NOT equal")
            yield self.new_request(resp)

    def get_station_data(self, resp):
        table = resp.xpath(u'//*[@id="grafico"]/table/tr[1]/td[2]/img')
        general_date = self.url_to_datetime(resp)

        res_set = list()
        for img in table:
            row_value = img.xpath(u"@title").extract_first()
            row_value = row_value.split(u" - ")
            value = row_value[0]
            hour = int(row_value[1].rstrip(u" hs."))
            if hour == 24:
                hour = 0

            pollutant_name = self.get_pollutant_name(resp.url)

            date_pollutant = general_date.replace(hour=hour)
            res = {
                pollutant_name: value,
                u"date": date_pollutant
            }
            res_set.append(res)

        pollutant_number = url_query_parameter(resp.url, u"contaminante")
        pollutant_number = int(pollutant_number)
        if pollutant_number > 3:
            print(u"YES I AM 3!!! |{0}|".format(pollutant_number))
            return self.validate_station_data(resp.meta)
        else:
            print(u"NOT YET |{0}|".format(pollutant_number))
            new_url = add_or_replace_parameter(resp.url, u"contaminante", pollutant_number + 1)
            result = {
                u"res" + str(pollutant_number): res_set
            }

            # check meta data
            results = resp.meta.get(u"results")
            if not results:
                results = dict()

            results.update(result)

            return Request(
                url=new_url,
                callback=self.get_station_data,
                meta={
                    u"results": results,
                    u"station_code": self.get_station_code(resp.url)
                }
            )

    def validate_station_data(self, meta):
        """validate dictionary from response META attribute"""
        results = meta.get(u"results")

        res1 = results.get(u"res1")
        res2 = results.get(u"res2")
        res3 = results.get(u"res3")

        df1 = DataFrame.from_dict(res1).sort_values(by=u"date")
        df2 = DataFrame.from_dict(res2).sort_values(by=u"date")
        df3 = DataFrame.from_dict(res3).sort_values(by=u"date")

        df = merge(df1, df2, on=u"date")
        df = merge(df, df3, on=u"date")

        # print(df)

        source_code = meta.get(u"station_code")

        for obs in df.itertuples():
            st_data = dict()
            co = Kind(self.name).get_dict(r_key=u"co", r_val=obs.co)
            pm10 = Kind(self.name).get_dict(r_key=u"pm10", r_val=obs.pm10)
            no2 = Kind(self.name).get_dict(r_key=u"no2", r_val=obs.no2)

            if co:
                st_data[co[u"key"]] = co[u"val"]
            if pm10:
                st_data[pm10[u"key"]] = pm10[u"val"]
            if no2:
                st_data[no2[u"key"]] = no2[u"val"]

            data_time = obs.date.to_datetime()
            data_time = data_time.replace(tzinfo=timezone(self.tz))

            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = source_code

                yield items

    def parse(self, response):
        yield self.get_station_data(response)


