# coding: utf-8
from scrapy import Spider, FormRequest
from ujson import loads
from datetime import datetime
from pytz import timezone


class IneaSpider(Spider):
    name = "br_inea"
    tz = "Brazil/East"
    start_urls = ("http://200.20.53.7/hotsiteinea/default.aspx",)

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {'pollution_app.middlewares.JSDownloader': 400}
    }

    def parse(self, response):
        yield FormRequest.from_response(
            response,
            formdata={
                'ddlHora': "14:00",
                'ddlDia': "27",
                'ddlMes': "9",
                "ddlRegiao": "-1",
                "ddlTipo": "-1",
                "ddlAno": "2016",
            },
            callback=self.parse
        )

    def parse_tags(self, response):
        # #ddlTipo
        # //*[@id="ddlDia"]/option[27]
        day = response.xpath('//*[@id="ddlDia"]/option[@selected="selected"]/@value').extract_first()
        # //*[@id="ddlHora"]
        hour = response.xpath('//*[@id="ddlHora"]/option[@selected="selected"]/@value').extract_first()
        region = response.xpath('//*[@id="ddlRegiao"]/option[@selected="selected"]/@value').extract_first()
        print(day)
        print(hour)
        print(region)