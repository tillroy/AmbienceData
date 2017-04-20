# -*- coding: utf-8

from datetime import datetime, timedelta
import time

import ujson
from dateutil import parser
from scrapy import Spider, Request
from pytz import timezone

from pollution_app.feature import Feature
from pollution_app.items import LayerContainer, AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class WundergroundSpider(Spider):
    name = u"all_wunderground_weather_forecast"

    # FIXME works only for california (tz)
    tz = u"US/Pacific"
    source = u"https://www.wunderground.com"

    custom_settings = {
        "ITEM_PIPELINES": {'pollution_app.pipelines.WeatherForecastPipeline': 300}
    }


    def start_requests(self):
        # FIXME add all 20 stations
        codes = (
            u"KNVGARDN52", u"MBPOC1", u"KNVRENO168", u"KCALAKEB2", u"KCAPOLLO31", u"MDEDN2", u"MBBNC1", u"KCABERKE105",
            u"KCASEBAS52", u"KCASANTA115", u"KCATRACY12", u"KCALAKEB2", u"KNZY", u"KCAFALLB30", u"MLCMSD",
            u"KCALOSAN538", u"KCACHINO14", u"KCAOXNAR21", u"KGXA"

            # u"KCANORTH45",
        )

        app_key = u"c5044610336c269a"

        href = u"http://api.wunderground.com/api/{app_key}/hourly/lang:US/q/pws:{station_id}.json"

        for code_value in codes:
            time.sleep(10)

            url = href.format(app_key=app_key, station_id=code_value)

            yield Request(
                url=url,
                callback=self.get_test,
                meta={u"code": code_value}
            )

    def get_value(self, item):
        res = None
        try:
            res = item["metric"]
        except KeyError:
            res = item["degrees"]
        except TypeError:
            res = item

        return res

    def get_test(self, resp):

        local_units = {
            u"temp": u"degc",
            u"snow": u"",
            u"wd": u"deg",
            u"temp_dew_p": u"degc",
            u"hum": u"%",
            u"sn_d": u"mm",
            u"pres": u"gpa",
            u"sky": u"%",
        }

        json = ujson.loads(resp.text)

        lc = LayerContainer()

        min_forecast_date = None
        for rec in json["hourly_forecast"]:
            fdate = parser.parse(rec.pop("FCTTIME")["pretty"])
            if min_forecast_date is None:
                min_forecast_date = fdate

            if fdate < min_forecast_date:
                min_forecast_date = fdate

            layer = dict()
            for key, val in rec.items():

                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                # print("record", record)
                pollutant.set_raw_name(key)
                pollutant.set_raw_value(self.get_value(val))

                try:
                    pollutant.set_raw_units(local_units[pollutant.get_name()])
                except KeyError:
                    if pollutant.get_name() is not None:
                        print(
                            "There is no such pollutant in local units list <<<<<<<<{0}>>>>>>".format(pollutant.get_name()))
                    else:
                        print(
                            "Name is None: <<<<<<<<{0}>>>>>>".format(
                                pollutant.get_name()))

                # print(
                #     "answare",
                #     pollutant.get_name(),
                #     # pollutant.get_value(),
                #     pollutant.get_units()
                # )

                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    layer[pollutant.get_name()] = pollutant.get_value()

            lc.add_layer(fdate, layer)

        curr_date = min_forecast_date - timedelta(hours=1)

        forecast_data = lc.get_layers()

        if forecast_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = curr_date
            items[u"forecast_data"] = forecast_data
            items[u"source"] = self.source
            items[u"source_id"] = resp.meta[u"code"]

            yield items
