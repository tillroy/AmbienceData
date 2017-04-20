# coding: utf-8

from datetime import datetime
import re

from scrapy import Spider, Request, Selector, FormRequest
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

script2 = """
function main(splash)
    assert(splash:go(splash.args.url))
    return splash:evaljs("document.title")
end
"""

class KansasSpider(Spider):
    name = u"us_kansas"
    source = u""
    tz = u""

    # start_urls = (u"http://keap.kdhe.state.ks.us/AirVision/Public/Viewer.aspx?GuiFavoriteID=586f374d-f341-e511-9408-24b6fdf95661",)

    def start_requests(self):
        url = u"http://keap.kdhe.state.ks.us/AirVision/Public/Viewer.aspx?GuiFavoriteID=586f374d-f341-e511-9408-24b6fdf95661"

        yield SplashRequest(
            url=url,
            # callback=self.parse_tags,
            callback=self.preparation,
            endpoint='execute',
            cache_args=['lua_source'],
            args={'lua_source': script},
            headers={'X-My-Header': 'value'},
        )

    def preparation(self, resp):
        print("TAGS1", resp.meta)
        # print("TAGS1", resp.cookiejar)

        form = resp.xpath(u'//*[@id="aspnetForm"]')
        form_name = form.xpath(u"./@name").extract_first()
        form_id = form.xpath(u"./@id").extract_first()
        form_action = form.xpath(u"./@action").extract_first().lstrip("../")
        print(form_action)

        # hidden = resp.xpath(u"//input[@type='hidden']")
        #
        # fm = dict()
        # for el in hidden:
        #     name = el.xpath(u"./@name").extract_first()
        #     value = el.xpath(u"./@value").extract_first()
        #     fm[name] = value

        fm = {
            # u"__EVENTARGUMENT": u"saveToDisk=format:html;",
            u"__EVENTARGUMENT": u"saveToWindow=format:csv;",
            u"__EVENTTARGET": u"tl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",

            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT5_PageNumber_VI": u"5",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L": u"5",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber": u"5",

            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"csv",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"csv",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Csv",

        }

        formdata1 = {
            u"__EVENTARGUMENT": u"saveToWindow=format:html;",
            u"__EVENTTARGET": u"tl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl": u"{&quot;activeTabIndex&quot;:1}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu": u"{&quot;selectedItemIndexPath&quot;:&quot;&quot;,&quot;checkedState&quot;:&quot;&quot;}",
            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT5_PageNumber_VI": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:0:0:0:-10000:-10000:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MasinContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L$State": u"{&quot;CustomCallback&quot;:&quot;&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT6$PageCount": u"25",
            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:430:250:1:68:165:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer": u"{&quot;drillDown&quot;:{},&quot;parameters&quot;:{},&quot;cacheKey&quot;:&quot;&quot;,&quot;currentPageIndex&quot;:0}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxErrorGrid": u"{&quot;keys&quot;:[],&quot;callbackState&quot;:&quot;BwQHAwIERGF0YQcnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwAHAAcAAgtGb3JtYXRTdGF0ZQcAAgVTdGF0ZQc/BwAHAAcABwAHAAIABQAAAIAJAgtUaW1lT2ZFcnJvcgcACQIAAgADBwQCAAcAAgEHAAcAAgACAQcABwAHAAcAAg1TaG93RmlsdGVyUm93CgIB&quot;,&quot;selection&quot;:&quot;&quot;}",
        }

        yield SplashFormRequest.from_response(
            response=resp,
            formid=form_id,
            formdata=formdata1,
            callback=self.parse_tags,
            dont_click=True,
            endpoint='execute',
            cache_args=['lua_source'],
            args={'lua_source': script},
        )


    def parse(self, response):


        formdata = {
            u"__EVENTTARGET": u"tl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",
            # u"__EVENTTARGET": u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",
            # u"__EVENTARGUMENT": response.xpath(u"//input[@id='__EVENTARGUMENT']/@value").extract_first(),
            u"__EVENTARGUMENT": u"saveToWindow=format:html;",
            u"__VSTATE_AV": response.xpath(u"//input[@id='__VSTATE_AV']/@value").extract_first(),
            u"__VIEWSTATE": response.xpath(u"//input[@id='__VIEWSTATE']/@value").extract_first(),
            u"__EVENTVALIDATION": response.xpath(u"//input[@id='__EVENTVALIDATION']/@value").extract_first(),
            u"DXScript": u"1_232,1_134,1_225,1_169,1_226,1_223,1_155,9_45,1_131,1_217,1_206,1_167,1_175,1_138,1_180,1_166,1_164,1_215,1_170,9_38,9_48,9_46,1_153,1_194,1_196",
            u"DXCss": u"1_33,1_18,0_3879,0_4037,0_4039,0_3877,0_4060,0_3883,0_3885,../Content/bootstrap.min.css,../Styles/Site.css,../images/AgilaireAV.ico",

            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl": u"{&quot;activeTabIndex&quot;:1}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu": u"{&quot;selectedItemIndexPath&quot;:&quot;&quot;,&quot;checkedState&quot;:&quot;&quot;}",
            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT5_PageNumber_VI": u"2",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber": u"2",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:0:0:0:-10000:-10000:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L$State": u"{&quot;CustomCallback&quot;:&quot;&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L": u"2",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT6$PageCount": u"25",
            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:430:250:1:68:165:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer": u"{&quot;drillDown&quot;:{},&quot;parameters&quot;:{},&quot;cacheKey&quot;:&quot;&quot;,&quot;currentPageIndex&quot;:1}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxErrorGrid": u"{&quot;keys&quot;:[],&quot;callbackState&quot;:&quot;BwQHAwIERGF0YQcnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwAHAAcAAgtGb3JtYXRTdGF0ZQcAAgVTdGF0ZQc/BwAHAAcABwAHAAIABQAAAIAJAgtUaW1lT2ZFcnJvcgcACQIAAgADBwQCAAcAAgEHAAcAAgACAQcABwAHAAcAAg1TaG93RmlsdGVyUm93CgIB&quot;,&quot;selection&quot;:&quot;&quot;}",
            # u"": u"",
        }

        formdata1 = {
            u"__EVENTARGUMENT": u"saveToWindow=format:html;",
            u"__EVENTTARGET": u"tl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl": u"{&quot;activeTabIndex&quot;:1}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu": u"{&quot;selectedItemIndexPath&quot;:&quot;&quot;,&quot;checkedState&quot;:&quot;&quot;}",
            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT5_PageNumber_VI": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:0:0:0:-10000:-10000:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MasinContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L$State": u"{&quot;CustomCallback&quot;:&quot;&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT6$PageCount": u"25",
            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDDState": u"{&quot;windowsState&quot;:&quot;0:0:-1:430:250:1:68:165:1:0:0:0&quot;}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer": u"{&quot;drillDown&quot;:{},&quot;parameters&quot;:{},&quot;cacheKey&quot;:&quot;&quot;,&quot;currentPageIndex&quot;:0}",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxErrorGrid": u"{&quot;keys&quot;:[],&quot;callbackState&quot;:&quot;BwQHAwIERGF0YQcnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwAHAAcAAgtGb3JtYXRTdGF0ZQcAAgVTdGF0ZQc/BwAHAAcABwAHAAIABQAAAIAJAgtUaW1lT2ZFcnJvcgcACQIAAgADBwQCAAcAAgEHAAcAAgACAQcABwAHAAcAAg1TaG93RmlsdGVyUm93CgIB&quot;,&quot;selection&quot;:&quot;&quot;}",
            u"DXScript": u"1_232,1_134,1_225,1_169,1_226,1_223,1_155,9_45,1_131,1_217,1_206,1_167,1_175,1_138,1_180,1_166,1_164,1_215,1_170,9_38,9_48,9_46,1_153,1_194,1_196",
            u"DXCss": u"1_33,1_18,0_3879,0_4037,0_4039,0_3877,0_4060,0_3883,0_3885,../Content/bootstrap.min.css,../Styles/Site.css,../images/AgilaireAV.ico",
        }

        fm = {
            # u"__EVENTARGUMENT": u"saveToDisk=format:html;",
            u"__EVENTARGUMENT": u"saveToWindow=format:html;",
            u"__EVENTTARGET": u"tl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportViewer",

            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT5_PageNumber_VI": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber$DDD$L": u"1",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT5$PageNumber": u"1",

            u"ctl00_ctl00_ctl00_MainContent_MainContent_ReportOutputPlaceHolder_uxTabControl_uxReportToolbar_Menu_ITCNT11_SaveFormat_VI": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat$DDD$L": u"html",
            u"ctl00$ctl00$ctl00$MainContent$MainContent$ReportOutputPlaceHolder$uxTabControl$uxReportToolbar$Menu$ITCNT11$SaveFormat": u"Html",

            u"DXScript": u"1_232,1_134,1_225,1_169,1_226,1_223,1_155,9_45,1_131,1_217,1_206,1_167,1_175,1_138,1_180,1_166,1_164,1_215,1_170,9_38,9_48,9_46,1_153,1_194,1_196",
            u"DXCss": u"1_33,1_18,0_3879,0_4037,0_4039,0_3877,0_4060,0_3883,0_3885,../Content/bootstrap.min.css,../Styles/Site.css,../images/AgilaireAV.ico",
        }
        # print(response.xpath(u'//*[@id="DXScript"]').extract_first())
        # fm[response.xpath(u'//*[@id="DXScript"]/@name').extract_first().decode()] = response.xpath(u'//*[@id="DXScript"]/@value').extract_first().decode(),
        # print(formdata)

        yield SplashFormRequest.from_response(
            # url=u"http://keap.kdhe.state.ks.us/AirVision/Modules/Reporting/ReportViewers/XtraReportViewer.aspx?dxrep_fake=&q=0a05224c-dc17-4166-9cf6-0fb063c315a7",
            response,
            formdata=fm,
            callback=self.parse_tags,
            dont_click=True,
            # endpoint='render.html',
        )

        # print(formdata[u"__EVENTVALIDATION"])
        # yield FormRequest(
        #     # 'http://keap.kdhe.state.ks.us/AirVision/Public/Viewer.aspx?GuiFavoriteID=586f374d-f341-e511-9408-24b6fdf95661',
        #     u'http://keap.kdhe.state.ks.us/AirVision/Modules/Reporting/ReportViewers/XtraReportViewer.aspx?dxrep_fake=&q=6274e0f7-6327-4ff8-a7bc-b8f305a2b76e',
        #     formdata=formdata,
        #     callback=self.parse_tags,
        #     cookies={u'ASP.NET_SessionId': u'kds5v250umdwkcnkvfnypidp'}
        # )

    def parse_tags(self, response):
        print("TAGS2", response.meta)
        print("TAGS2", response.url)
        print("TAGS2", response.headers)
        # print(u"RES", response.text)

        # for key, val in response.meta["splash"]["args"]:
        #     record = '{0} : {:100.100}'.format(key, val)
        #     print(record)
        res = response.meta["splash"]["args"]["body"]

        open("test_res.html", 'w').write(response.text.encode("utf-8"))
        open("test_res_headers.html", 'w').write(res.encode("utf-8"))
