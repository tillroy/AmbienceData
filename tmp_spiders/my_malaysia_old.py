# coding: utf-8
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone
from ujson import loads as js_loads

from pollution_app.aqi import Aqi
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from re import findall, sub, DOTALL
from scrapy import Spider


class MalaysiaSpider(Spider):
    name = u"my_malaysia"
    tz = u"Asia/Kuala_Lumpur"
    source = u"http://apims.doe.gov.my"
    # st_data = (u"http://apims.doe.gov.my/v2/index.html",)
    st_data = (u"http://apims.doe.gov.my/v2/table.html",)
    start_urls = st_data

    def get_value(self, string):
        regex = u"<h2>(\d*.?)</h2>"
        substr = findall(regex, string)
        try:
            _substr = substr[0]
            aqi = _substr[:-1]
            # print("aqi", aqi)

            names = [u"aqi"]
            if u"*" in _substr:
                names.append(u"pm10")
            elif u"a" in _substr:
                names.append(u"so2")
            elif u"b" in _substr:
                names.append(u"no2")
            elif u"c" in _substr:
                names.append(u"o3")
            elif u"d" in _substr:
                names.append(u"co")

            # print("names", names)

            st_data = dict()
            for name in names:
                val = aqi
                if name != u"aqi":
                    # конвертуєму в з начення якщо це можливо
                    val = Aqi().aqi_to_val(float(aqi), name)

                _tmp_dict = Kind(self.name).get_dict(r_key=name, r_val=val)
                # print("res", _tmp_dict)
                if _tmp_dict:
                    st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            return st_data

        except IndexError:
            return None

    @staticmethod
    def get_st_name(string):
        regex = u"><h2>(.+)</h2>"
        substr = findall(regex, string)
        if substr:
            sub = substr[0]
        else:
            sub = None

        return sub

    @staticmethod
    def get_page(resp):
        regex = u"values:\[.*\],\s*options"

        page = resp.xpath(u"//script").extract()
        print(page)
        # для України
        js_info = page[26]
        # для всіх іншик країн
        # js_info = page[25]
        # print(js_info)
        row_data = findall(regex, js_info, DOTALL)
        row_data = sub(ur"\s+", u" ", row_data[0])
        data = row_data.lstrip(u"values:").rstrip(u",options")
        data = data.replace(u"latLng:", u'"latLng":')
        data = data.replace(u"data:'", u'"data":"')
        data = data.replace(u"', options:", u'", "options":')
        data = data.replace(u"icon: '", u'"icon": "')
        data = data.replace(u"png'}", u'png"}')
        data = data.replace(u"},}", u"}}")
        data = data.replace(u"\\'", u"'")
        data = data.replace(u" ],", u" ]")

        print(data)

        data = js_loads(data)
        return data

    def get_date(self, resp):
        date = resp.xpath(u'//*[@id="topbar"]/div/ol/li[1]/a/text()').extract_first()
        row_date = date.lstrip(u"LATEST UPDATE : ")
        dt = parse(row_date)
        new_dt = dt.replace(tzinfo=timezone(self.tz))

        return new_dt

    def get_st_data(self, resp):
        data_date = self.get_date(resp)

        for st in self.get_page(resp):
            st_id = self.get_st_name(st[u"data"])

            st_data = self.get_value(st[u"data"])

            if st_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_date
                items[u"data_value"] = st_data
                items[u"source"] = self.source
                items[u"source_id"] = st_id
                yield items

    def parse(self, response):
        for item in self.get_st_data(response):
            yield item


if __name__ == "__main__":
    value = "68*"
    v = '<table><tr><td style=\'width:60px; text-align:center; color:white; padding:5px; border:1px solid grey; background-color:rgb( 46, 139, 87)\'>API<h2>{0}</h2>Moderate</td><td style=\'padding-left:20px; text-align:center;\'><h2>Kangar</h2>04 Apr 2016 - 8:00PM<br></td></tr></table>'.format(value)
    res = MalaysiaSpider().get_value(v)
    print(res)
