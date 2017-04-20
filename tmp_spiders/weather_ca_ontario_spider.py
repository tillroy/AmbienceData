# coding: utf-8
import scrapy
import re


class WeatherOntarioSpider(scrapy.Spider):
    name = 'weather_ca_ontario'
    pos_urls = (
        'http://weather.gc.ca/past_conditions/index_e.html?station=tnk',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wyw',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yyw',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wch',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yat',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wrk',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tbt',
        'http://weather.gc.ca/past_conditions/index_e.html?station=otl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ytl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ybn',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tbf',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tbo',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wwb',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xca',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wci',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yld',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yck',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wnc',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wco',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wwx',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xdi',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yhd',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xea',
        'http://weather.gc.ca/past_conditions/index_e.html?station=txr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yxr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xet',
        'http://weather.gc.ca/past_conditions/index_e.html?station=zel',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tag',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yer',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ywa',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ygq',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wgd',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tze',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yze',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wnl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tsb',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ysb',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tgt',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xhm',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yhm',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xha',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xka',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yyu',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xke',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tkr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yqk',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wbe',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ygk',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xki',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wgl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xbi',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tls',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wlf',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ylh',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wkk',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yxu',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wps',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ysp',
        'http://weather.gc.ca/past_conditions/index_e.html?station=try',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tck',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xzc',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ymo',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wls',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yqa',
        'http://weather.gc.ca/past_conditions/index_e.html?station=zmd',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wnz',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yyb',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ykp',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yoo',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xoa',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yow',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xpc',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wwn',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ypo',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tpm',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ypq',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tpq',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wpl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ypl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xpt',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wqp',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wpc',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wwz',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wcj',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tra',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yrl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xrg',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wtx',
        'http://weather.gc.ca/past_conditions/index_e.html?station=zsj',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tzr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yzr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yam',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yxl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wnb',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ysn',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ztb',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yqt',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tms',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yts',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ttr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ykz',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ytz',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xto',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yyz',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ytr',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wdv',
        'http://weather.gc.ca/past_conditions/index_e.html?station=tux',
        'http://weather.gc.ca/past_conditions/index_e.html?station=xvn',
        'http://weather.gc.ca/past_conditions/index_e.html?station=ykf',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wec',
        'http://weather.gc.ca/past_conditions/index_e.html?station=twl',
        'http://weather.gc.ca/past_conditions/index_e.html?station=wmz',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yvv',
        'http://weather.gc.ca/past_conditions/index_e.html?station=yqg'
    )

    start_urls = ['http://weather.gc.ca/provincialsummary_table/index_e.html?prov=on&page=hourly']
    # start_urls = pos_urls
    # start_urls = ('http://weather.gc.ca/past_conditions/index_e.html?station=yqg',)

    # tz = 'EST' old
    tz = 'EDT'

    def parse(self, response):
        self.get_st_data(response)

    def get_st_data(self, resp):
        # row = resp.xpath('//*[@id="hourtable"]/tbody/tr[1]/td[1]/a/text()').extract_first()
        rows = resp.xpath('//*[@id="hourtable"]/tbody/tr')
        print(len(rows))
        for row in rows:
            name = row.xpath('td[1]/a/text()').extract_first()
            # time = row.xpath('td[2]/text()').extract_first()
            # temperature = row.xpath('td[4]/text()').re_first('.*\((\d*.{0,1}\d*)')
            wind_direction = row.xpath('td[6]/abbr/text()').extract_first()
            if wind_direction is not 'calm':
                wind_speed = row.xpath('td[6]/text()').extract()
                try:
                    wind_speed = wind_speed[1]
                    regex_ws = '.*(\d?\d\.?\d\d?).*'
                    wind_speed = re.findall(regex_ws, wind_speed)
                except IndexError:
                    wind_speed = None
            else:
                wind_speed = None

            # rel_humidity = row.xpath('td[7]/text()').extract_first()
            # pressure = row.xpath('td[8]/text()').extract_first()
            # visibility = row.xpath('td[9]/text()').extract_first()
            # print(name, time, temperature, wind, rel_humidity, pressure, visibility)
            print(name, wind_speed)
            break