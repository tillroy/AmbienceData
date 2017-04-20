# coding: utf-8
from scrapy import Spider
from ujson import loads as js_loads


class OpenaqSpider(Spider):
    name = 'all_openaq'
    st_data = ('https://api.openaq.org/v1/latest?country=CA&limit=1000',)
    # st_data = ('https://api.openaq.org/v1/latest?limit=1000',)
    start_urls = st_data
    tz = ''

    def get_st_data(self, resp):
        json = js_loads(resp.body)
        data = json['results']
        print(len(data))
        for st in data:
            try:
                coord = st['coordinates']
                lon = coord['longitude']
                lat = coord['latitude']
                location = repr(st['location'])
                if '\ufffd' in location:
                    location = location.replace('\ufffd', 'c')
                    location = location.lstrip("u'").rstrip("'")
                else:
                    location = location.lstrip("u'").rstrip("'")
                country = st['country']
                city = st['city']
                # print(repr(location))
                res = str(lon) + ', ' + str(lat) + ', ' + country + ', ' + city + ', ' + location + '\n'
                open('openaq_pos.txt', 'a').write(res)
            except KeyError:
                pass

    def parse(self, response):
        self.get_st_data(response)