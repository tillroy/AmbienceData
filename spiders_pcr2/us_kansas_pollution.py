# coding: utf-8

from datetime import datetime
import re
from io import StringIO

from scrapy import Spider
from scrapy_splash import SplashRequest, SplashFormRequest

from dateutil import parser
from pytz import timezone
import ujson
import pandas as pd

from pollution_app.feature import Feature
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


script = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    })
  assert(splash:wait(0.5))

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
  }
end
"""


class KansasSpider(Spider):
    name = u"us_kansas_pollution"
    source = u"http://keap.kdhe.state.ks.us"
    tz = u"US/Central"

    def start_requests(self):
        url = u"http://keap.kdhe.state.ks.us/AirVision/Public/Viewer.aspx?GuiFavoriteID=586f374d-f341-e511-9408-24b6fdf95661"

        yield SplashRequest(
            url=url,
            # callback=self.parse_tags,
            callback=self.preparation,
            endpoint=u"execute",
            cache_args=[u"lua_source"],
            args={
                u"lua_source": script,
            }
        )

    def preparation(self, resp):
        form = resp.xpath(u'//*[@id="aspnetForm"]')
        form_id = form.xpath(u"./@id").extract_first()

        formdata = {
            # u"__EVENTARGUMENT": u"saveToWindow=format:html;",
            u"__EVENTARGUMENT": u"saveToWindow=format:csv;",
            u"__EVENTTARGET": u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",

            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT5_PageNumber_VI": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT6$PageCount": u"25",

            # u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"html",
            # u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"html",
            # u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Html",

            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"csv",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"csv",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Csv",

            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:0:0:0:-10000:-10000:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L$State": u"{&quot;CustomCallback&quot;:&quot;&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:430:140:1:68:165:1:0:0:0&quot;}",

            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl": u"{&quot;activeTabIndex&quot;:1}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu": u"{&quot;selectedItemIndexPath&quot;:&quot;&quot;,&quot;checkedState&quot;:&quot;&quot;}",

            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer": u"{&quot;drillDown&quot;:{},&quot;parameters&quot;:{},&quot;cacheKey&quot;:&quot;&quot;,&quot;currentPageIndex&quot;:0}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxErrorGrid": u"{&quot;keys&quot;:[],&quot;callbackState&quot;:&quot;BwQHAwIERGF0YQcnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwAHAAcAAgtGb3JtYXRTdGF0ZQcAAgVTdGF0ZQc/BwAHAAcABwAHAAIABQAAAIAJAgtUaW1lT2ZFcnJvcgcACQIAAgADBwQCAAcAAgEHAAcAAgACAQcABwAHAAcAAg1TaG93RmlsdGVyUm93CgIB&quot;,&quot;selection&quot;:&quot;&quot;}",
        }

        yield SplashFormRequest.from_response(
            response=resp,
            formid=form_id,
            formdata=formdata,
            callback=self.get_station_data,
            # callback=self.parse_tags,
            dont_click=True,
            endpoint=u"execute",
            cache_args=[u"lua_source"],
            args={
                u"http_method": u"POST",
                u"headers": {
                    u"Content-Type": u"application/x-www-form-urlencoded",
                },
                u"lua_source": script,
            }
        )

    @staticmethod
    def get_clean_data(resp):
        raw_data = resp.xpath(u"//pre/text()").extract_first()
        data = re.findall(u",,,,,,Daily Summary Report,,,,,,,,\n(.*?)Total,,,,,,,,,,,,,,", raw_data, re.DOTALL)
        all_data = list()
        for table in data:
            header = re.findall(u"^.+\n.+\n.+\n", table)[0]

            # units dict
            _names = re.findall(u"^.+\n(.+)\n", header)[0]
            _units = re.findall(u"(.+)$", header)[0]
            res = zip(_names.split(u","), _units.split(u","))
            units = {x[0]: x[1] for x in res if x[0] != u""}
            # units dict

            column_names = u"Hour" + re.findall(u"^.+\n(.+\n)", header)[0]
            site_name = re.findall(u"Site:,(.+?),", header)[0]
            table_date = re.findall(u"(\d+?/\d+?/\d+?)\,", header)[0]

            raw_table_body = re.sub(u"^.+\n.+\n.+\n", u"", table)
            table_body = re.sub(u".+\n.+\n.+\n.+\n.+\n$", u"", raw_table_body)

            table_body = column_names + table_body

            df = pd.read_csv(StringIO(table_body))
            df = df.dropna(axis=1, thresh=1)
            df = df.dropna(axis=0, thresh=2)

            # print(df)

            df[u"Hour"] = table_date + u" " + df[u"Hour"]
            df[u"Hour"] = [parser.parse(x) for x in df[u'Hour']]

            valid_table_data = list()
            for rec in df.iterrows():
                row = rec[1].to_dict()
                data_time = row.pop(u"Hour")
                record = [
                    {
                        u"pollutant_name": u" ".join(x[0].split()),
                        u"pollutant_value": x[1],
                        u"date": data_time,
                        u"station_name": u" ".join(site_name.split()),
                        u"unit": units.get(x[0])
                    } for x in row.items()]

                valid_table_data.extend(record)

            all_data.extend(valid_table_data)

        df_all = pd.DataFrame(all_data)

        return df_all
        # return None

    def get_station_data(self, resp):
        all_data = self.get_clean_data(resp)

        current_data_time = all_data[u"date"].max()
        curr_all_data = all_data[all_data[u"date"] == current_data_time]

        # idx = curr_all_data.groupby(by=u"pollutant_name")[u"date"].transform(max) == curr_all_data[u"date"]
        # data = curr_all_data[idx].copy()
        data = curr_all_data
        data = data[[u"station_name", u"pollutant_name", u"pollutant_value", u"unit"]]
        grouped = data.groupby(by=u"station_name")

        for name, gr in grouped:
            station_data = dict()
            station_id = None

            for record in gr.itertuples(index=False):
                if station_id is None:
                    station_id = record[0]

                pollutant_name = record[1]
                pollutant_value = record[2]
                pollutant_units = record[3]

                # print(station_id, pollutant_name, pollutant_value, pollutant_units)

                pollutant = Feature(self.name)
                pollutant.set_source(self.source)
                pollutant.set_raw_name(pollutant_name)
                pollutant.set_raw_value(pollutant_value)
                pollutant.set_raw_units(pollutant_units)
        #
                # print("answare", station_id, pollutant.get_name(), pollutant.get_value(), pollutant.get_units())
                if pollutant.get_name() is not None and pollutant.get_value() is not None:
                    station_data[pollutant.get_name()] = pollutant.get_value()

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = pd.to_datetime(current_data_time).replace(tzinfo=timezone(self.tz))
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items