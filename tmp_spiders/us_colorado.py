#  coding: utf-8
import re
import datetime

from scrapy import Spider, Request
from dateutil.parser import parse
from pytz import timezone

from pollution_app.pollution import Kind
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE


class ColoradoSpider(Spider):
    name = u"us_colorado"
    source = u"http://www.colorado.gov"
    start_urls = (u"http://www.colorado.gov/airquality/site_description.aspx",)

    def parse(self, response):
        self.get_st_pos(response)

        # stations = response.xpath('//*[@id="divSites"]/p')
        # base = 'http://www.colorado.gov/airquality/site.aspx?aqsid='
        # for st in stations:
        #     #  self.make_sql_pos_file(st)
        #
        #     st_id = st.xpath('text()').re(r'AQS ID:\s*(.*)')
        #     open(u"colorado_stations_id", u"a").write(str(st_id[0]) + '\n')
        #     href = str(st_id[0])
        #     url = base + href
        #
        #     yield Request(
        #         url,
        #         callback=self.parse_station)

    @staticmethod
    def get_st_pos(st):
        st_info = {
            'source': 'http://www.colorado.gov'
        }

        st_id = st.xpath('text()').re(r'AQS ID:\s*(.*)')
        lat = st.xpath('text()').re(r'Latitude:\s*(.*)')
        lot = st.xpath('text()').re(r'Longitude:\s*(.*)')
        address = st.xpath('text()').extract()
        address = str(address[2])

        if lat:
            st_info['lat'] = float(lat[0])
        else:
            st_info['lat'] = '-'

        if lot:
            st_info['lon'] = float(lot[0])
        else:
            st_info['lon'] = '-'

        if st_id:
            st_info['source_id'] = str(st_id[0])
        else:
            st_info['source_id'] = '-'

        if address:
            st_info['address'] = address
        else:
            st_info['address'] = '-'

        res = ";".join((st_info["source_id"], st_info["address"], st_info["lon"], st_info["lat"], "\n"))

        open(u"colorado_stations.txt", u"a").write(res)

        return st_info

    def make_sql_pos_file(self, st):
        st_info = self.get_st_pos(st)
        if (st_info['lon'] is not None or st_info['lat'] is not None) and st_info['source_id'] is not None:
            open('us_st_pos_colorado.sql', 'a').write('''
                INSERT INTO station(lon, lat, address, source_id, source)
                VALUES({0},{1},{2},{3},{4});
                '''.format(
                    st_info['lon'],
                    st_info['lat'],
                    "'" + str(st_info['address']) + "'",
                    "'" + str(st_info['source_id']) + "'",
                    "'" + str(st_info['source']) + "'")
                    )

    def parse_station(self, response):
        regex_station = 'aqsid=(.+)'
        st_id = re.findall(regex_station, response.url)
        st_id = st_id[0]
        # print(st_id)

        #  get date from title
        title = response.xpath('//*[@id="pTitle"]/text()').re('(\d+[/. ]?\d+[/. ]?\d+)')
        data_date = str(title[0])
        #  print(data_date)

        table = response.xpath('//*[@id="divSummaryData"]/table//tr')
        col_names = response.xpath('//*[@id="divSummaryData"]/table//tr[1]/td')

        #  get names of the colums
        names = []
        for name in col_names:
            tmp_name = name.xpath('text()').re('(.+?)\s+')
            try:
                n = tmp_name[0]
            except IndexError:
                n = 'HOUR'
            names.append(n)
        # print(names)

        #  get data from table
        #  value of each row
        tmp_data = []
        for row in table:
            # print('length',len(row))
            # не бере до уваги те що значення може бути відсутнім а наступне присутнім
            # col = row.xpath('td/text()').extract()

            _col = row.xpath('td')
            new_col = []
            for el in _col:
                text = el.xpath('text()').extract()
                try:
                    new_col.append(text[0])
                except:
                    new_col.append('')

            if len(new_col) == len(names):
                #  якщо значення в рядку пусті то не додавати його
                empty = 0
                for el in new_col:
                    if el == '':
                        empty += 1

                #  якщо різниця пустих значень і всіх значень рівна 1 то все ок
                #  1 значить що немусте тільки найменування години
                diff = len(new_col) - empty
                #  print(diff)
                if diff != 1:
                    tmp_data.append(new_col)
        #print(tmp_data, len(tmp_data))

        #  get the newest row in the table
        gen_data = []
        try:
            # видаляємо перший рядок в якому назви
            tmp_data.pop(0)

            last_ch = tmp_data[len(tmp_data)-1]
            gen_data = zip(names, last_ch)
            #  print(last_ch)
            #  print(gen_data)
        except IndexError:
            pass
        #print(gen_data)

        if gen_data:
            st_data = {}
            for val in gen_data:
                #  print(val[0])

                if val[0] == 'HOUR':
                    data_time = str(val[1])
                    data_date += ' ' + data_time
                    dt = parse(data_date)
                    new_dt = dt.replace(tzinfo=timezone('MST'))

                #  recognize key val from table
                _tmp_dict = Kind(self.name).get_dict(r_key=val[0], r_val=val[1])
                if _tmp_dict:
                    st_data[_tmp_dict['key']] = _tmp_dict['val']

            if st_data:
                items = AppItem()
                items['scrap_time'] = datetime.datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items['data_time'] = new_dt
                items['data_value'] = st_data
                items['source'] = self.source
                items['source_id'] = st_id

                return items
