# coding: utf-8
from scrapy import Spider, FormRequest
from ujson import loads
from datetime import datetime
from pytz import timezone


class IneaSpider(Spider):
    name = "br_inea"
    tz = "Brazil/East"
    # start_urls = ("http://200.20.53.7/hotsiteinea",)

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {'pollution_app.middlewares.JSDownloader': 400,}
    }

    @staticmethod
    def add_zero(x):
        x = str(x)
        if len(x) == 1:
            x = "0" + x
        return x

    def get_now_time(self):
        time_in_br = datetime.now(timezone(self.tz))
        br_now = {
            "hour": self.add_zero(time_in_br.hour) + ":00",
            "day": self.add_zero(time_in_br.day-2),
            "month": str(time_in_br.month),
            "year": str(time_in_br.year)
        }

        return br_now

    def start_requests(self):
        br_now = self.get_now_time()
        print(br_now)
        yield FormRequest(
            url="http://200.20.53.7/hotsiteinea/default.aspx",
            callback=self.parse,
            formdata={
                'ddlHora': br_now["hour"],
                'ddlDia': br_now["day"],
                'ddlMes': br_now["month"],
                "ddlRegiao": "-1",
                "ddlTipo": "-1",
                "ddlAno": br_now["year"],
                "ScriptManager1": "uppGeral | botao",
                "botao": "Ok",
                "__ASYNCPOST": "true",
                "__VIEWSTATE": "/wEPDwUKMTIxMTQ0NTI3MQ8WAh4FZXhpYmVnFgICAQ9kFgICAw9kFgJmD2QWDAIBDxAPFgIeC18hRGF0YUJvdW5kZ2QQFQUHUmVnacOjbw5NZWRpbyBQYXJhw61iYQ1NZXRyb3BvbGl0YW5hEE5vcnRlIEZsdW1pbmVuc2UPT3V0cmFzIFJlZ2nDtWVzFQUCLTEOTWVkaW8gUGFyYcOtYmENTWV0cm9wb2xpdGFuYRBOb3J0ZSBGbHVtaW5lbnNlD091dHJhcyBSZWdpw7VlcxQrAwVnZ2dnZ2RkAgMPEA8WAh8BZ2QQFQMEVGlwbwtBdXRvbcOhdGljYQ9TZW1pYXV0b23DoXRpY2EVAwItMQtBdXRvbcOhdGljYQ9TZW1pYXV0b23DoXRpY2EUKwMDZ2dnZGQCBQ8QDxYCHwFnZBAVGAUwMDowMAUwMTowMAUwMjowMAUwMzowMAUwNDowMAUwNTowMAUwNjowMAUwNzowMAUwODowMAUwOTowMAUxMDowMAUxMTowMAUxMjowMAUxMzowMAUxNDowMAUxNTowMAUxNjowMAUxNzowMAUxODowMAUxOTowMAUyMDowMAUyMTowMAUyMjowMAUyMzowMBUYBTAwOjAwBTAxOjAwBTAyOjAwBTAzOjAwBTA0OjAwBTA1OjAwBTA2OjAwBTA3OjAwBTA4OjAwBTA5OjAwBTEwOjAwBTExOjAwBTEyOjAwBTEzOjAwBTE0OjAwBTE1OjAwBTE2OjAwBTE3OjAwBTE4OjAwBTE5OjAwBTIwOjAwBTIxOjAwBTIyOjAwBTIzOjAwFCsDGGdnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2RkAgcPEA8WAh8BZ2QQFR4CMDECMDICMDMCMDQCMDUCMDYCMDcCMDgCMDkCMTACMTECMTICMTMCMTQCMTUCMTYCMTcCMTgCMTkCMjACMjECMjICMjMCMjQCMjUCMjYCMjcCMjgCMjkCMzAVHgIwMQIwMgIwMwIwNAIwNQIwNgIwNwIwOAIwOQIxMAIxMQIxMgIxMwIxNAIxNQIxNgIxNwIxOAIxOQIyMAIyMQIyMgIyMwIyNAIyNQIyNgIyNwIyOAIyOQIzMBQrAx5nZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dkZAIJDxAPFgIfAWdkEBUMB2phbmVpcm8JZmV2ZXJlaXJvBm1hcsOnbwVhYnJpbARtYWlvBWp1bmhvBWp1bGhvBmFnb3N0bwhzZXRlbWJybwdvdXR1YnJvCG5vdmVtYnJvCGRlemVtYnJvFQwBMQEyATMBNAE1ATYBNwE4ATkCMTACMTECMTIUKwMMZ2dnZ2dnZ2dnZ2dnFgECCGQCCw8QDxYCHwFnZBAVBQQyMDE2BDIwMTUEMjAxNAQyMDEzBDIwMTIVBQQyMDE2BDIwMTUEMjAxNAQyMDEzBDIwMTIUKwMFZ2dnZ2dkZGQ2ofkotaOBQDDItq1zriajfqqcEA==",
                "__EVENTVALIDATION": "/wEWUgLf1qkjAs2U9vcIAtK6lsEMAoiI98sGAtOfrQUChrSXhQQCtYv4kQcCm66sgQECvqLD/QcC+LmduwIC+LmZugIC+LmFuQIC+LmBuAIC+LmNvwIC+LmJvgIC+Lm1vQIC+LmxvAIC+Ln9jAIC+Ln5gwIC27mduwIC27mZugIC27mFuQIC27mBuAIC27mNvwIC27mJvgIC27m1vQIC27mxvAIC27n9jAIC27n5gwICurmduwICurmZugICurmFuQICurmBuAICl67b4A0Cl67f4A0Cl67j4A0Cl67n4A0Cl67r4A0Cl67v4A0Cl67z4A0Cl6634w0Cl6674w0CiK7X4A0CiK7b4A0CiK7f4A0CiK7j4A0CiK7n4A0CiK7r4A0CiK7v4A0CiK7z4A0CiK634w0CiK674w0Cia7X4A0Cia7b4A0Cia7f4A0Cia7j4A0Cia7n4A0Cia7r4A0Cia7v4A0Cia7z4A0Cia634w0Cia674w0Ciq7X4A0Ck8GBiw4CnK6r5QICna6r5QICnq6r5QICn66r5QICmK6r5QICma6r5QICmq6r5QICi66r5QIChK6r5QICnK7r5gICnK7n5gICnK7j5gICzdaS5w4CzdbmywUCzdb6rgwCzdbOlQsCzdai+AMC8/aalAO/JHFtb59bw7SgotInIDN+YrFnhw=="
            },
            method="POST"

        )

    @staticmethod
    def get_station_info(req):
        script_body = req.xpath('//*[@id="form1"]/script[4]/text()').extract_first()
        script_body = script_body.lstrip(u'\r\n//<![CDATA[\r\nsetEstacoes("[')
        script_body = script_body.rstrip(u'", "False");Sys.Application.initialize();\r\n//]]>\r\n\']')
        observations = script_body.split(u"],[")

        for record in observations:
            record = record.replace(u"'", u'"')
            record = u"[" + record + u"]"
            record = loads(record)

            station_data = {
                "station_name": record[0],
                "pm10": int(record[10]),
                "so2": int(record[11]),
                "no2": int(record[12]),
                "o3": int(record[13]),
                "co": int(record[14]),
                "pts": int(record[15])
            }

            print(station_data)

    def get_station_data(self, req):
        # scripts = req.xpath('/html/head/script')
        print(req.headers)

    def parse(self, response):
        self.get_station_data(response)