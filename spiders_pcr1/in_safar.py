# coding: utf-8
import datetime
import pytz
import ujson
from dateutil.parser import parse

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

import re
import scrapy
from pollution_app.pollution import Kind
from scrapy.selector import Selector


class safarSpider(scrapy.Spider):
    name = u"in_safar"
    tz = u"Asia/Calcutta"
    source = u"http://pune.safar.tropmet.res.in"
    start_urls = (u"http://pune.safar.tropmet.res.in/AirQualityStatus.aspx?Id=1",)


    @staticmethod
    def get_page(resp):
        js_info = resp.xpath(u"//script")
        # print(js_info)
        scrt = js_info[len(js_info)-3]
        scrt = scrt.xpath(u"text()").extract_first()
        #  переробляємо кавички оскільки вони некоректні для json парсінга
        res = re.sub(ur"\s+", u" ", scrt)
        res = res.strip(u" var markers = ")
        res = res.rstrip(u";")
        string = res.replace(u"'", u"||")
        string = string.replace(u'"', u"'")
        string = string.replace(u"||", u'"')
        string = string.replace(u"'title':", u'"title":')
        string = string.replace(u"'lat':", u'"lat":')
        string = string.replace(u"'lng':", u'"lon":')
        string = string.replace(u"'description':", u'"description":')

        json = ujson.loads(string)
        return json

    def get_st_pos(self, resp):
        json = self.get_page(resp)
        for obj in json:
            st_info = dict()

            if obj[u"lat"]:
                st_info[u"lat"] = obj[u"lat"]
            else:
                st_info['lat'] = None

            if obj['lon']:
                st_info['lon'] = obj['lon']
            else:
                st_info['lon'] = None

            desc = html.fromstring(obj['description'])
            name = desc.xpath('tr[1]/td/text()')

            if name:
                st_info['source_id'] = str(name[0])
            else:
                st_info['source_id'] = None

            st_info['source'] = 'http://pune.safar.tropmet.res.in'

            if name:
                st_info['address'] = str(name[0])
            else:
                st_info['address'] = '-'

            yield st_info

    def make_pos_sql_file(self, resp):
        """
        make an sql file for inserting data to the table
        """
        st_info = self.get_st_pos(resp)
        for el in st_info:
            if (el['lon'] is not None or el['lat'] is not None) and el['source_id'] is not None:
                open('add_in_st_pos_safar.sql', 'a').write('''
                INSERT INTO scrapper_station(lon, lat, address, source_id, source)
                VALUES({0},{1},{2},{3},{4});
                '''.format(
                    el['lon'],
                    el['lat'],
                    "'" + str(el['address']) + "'",
                    "'" + str(el['source_id']) + "'",
                    "'" + str(el['source']) + "'")
                )

    def get_st_data(self, resp):
        json = self.get_page(resp)

        for obj in json:
            body = Selector(text=obj[u"description"])
            rows = body.xpath(u"//html/body/table/tr[3]/td/table/tr")
            st_id = str(body.xpath(u"//html/body/table/tr[1]/td/text()").extract_first())

            data_date = body.xpath(u"//html/body/table/tr[2]/td/text()").extract_first()
            data_date = str(data_date.rstrip(u" (IST)"))
            data_date = parse(data_date)
            # print(data_date)
            new_dt = data_date.replace(tzinfo=pytz.timezone(self.tz))

            st_data = dict()
            for row in rows:
                col = row.xpath(u"td/text()").extract()
                try:
                    _name = col[0]
                    _val = col[1]
                    _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                    if _tmp_dict:
                        st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]
                except IndexError:
                    pass

            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.datetime.now(tz=pytz.timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = new_dt
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = st_id
                yield items

    def parse(self, response):
        # self.get_st_data(response)
        for item in self.get_st_data(response):
            yield item
