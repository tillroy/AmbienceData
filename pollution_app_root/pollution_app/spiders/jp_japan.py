# coding: utf-8
from datetime import datetime, timedelta
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE

from pollution_app.pollution import Kind
from scrapy import Spider


def get_str_date():
    tz = u"Japan"
    # потррібно для динамічної підстановки в URL для кравлінга
    place_time = datetime.now(tz=timezone(tz))
    delta = timedelta(minutes=30)
    place_time -= delta
    year = unicode(place_time.year)
    month = unicode(place_time.month)
    if len(month) == 1:
        month = u"0" + month

    day = unicode(place_time.day)
    if len(day) == 1:
        day = u"0" + day

    hour = int(place_time.hour)-1
    # hour = str(place_time.hour)
    hour = unicode(hour)
    if len(hour) == 1:
        hour = u"0" + hour

    date = year + month + day
    return {u"date": date, u"hour": hour}


class JapanSpider(Spider):
    # station position source
    # http://www.nies.go.jp/igreen/tm_down.html
    # http://www.env.go.jp/air/osen/pm/info.html
    name = u"jp_japan"
    tz = u"Japan"
    source = u"http://soramame.taiki.go.jp"

    date = get_str_date()

    url = u"http://soramame.taiki.go.jp/Gazou/Hyou/AllMst/{0}/hp{0}{1}{2}.html"
    start_urls = list()
    for el in range(1, 48):
        el = unicode(el)
        if len(el) == 1:
            el = u"0" + unicode(el)

        url = url.format(date[u"date"], date[u"hour"], el)
        start_urls.append(url)

    def get_st_data(self, resp):
        col_names = (u"SO2", u"NO", u"NO2", u"NOX", u"CO", u"OX", u"NMHC", u"CH4", u"THC", u"SPM", u"PM2.5", u"SP",
                     u"WS", u"TEMP", u"HUM")
        data_time = parse(self.date[u"date"] + u" " + self.date[u"hour"]).replace(tzinfo=timezone(self.tz))

        rows = resp.xpath(u"//table/tr")
        for row in rows:
            col = row.xpath(u"td/text()").extract()
            col.pop(1)
            col.pop(13)

            station_id = col[0]

            col = col[1:]
            data = zip(col_names, col)

            station_data = dict()
            for st in data:
                _name = st[0]
                _val = st[1]
                _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)
                if _tmp_dict:
                    station_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

            # print(station_data)

            if station_data:
                items = AppItem()
                items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items[u"data_time"] = data_time
                items[u"data_value"] = station_data
                items[u"source"] = self.source
                items[u"source_id"] = station_id

                yield items

    def parse(self, response):
        # self.get_st_data(response)
        for st in self.get_st_data(response):
            yield st
