# coding: utf-8
import StringIO
from datetime import datetime
from dateutil.parser import parse
from pytz import timezone

from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pollution_app.pollution import Kind
from scrapy import Spider


class FimistonSpider(Spider):
    name = 'au_fimiston'
    tz = 'Australia/West'
    station_data = ('http://superpit.com.au/reports/KCGM%20Dust%20Report.pdf',)
    start_urls = station_data

    def get_date(self, row_date):
        data_time = parse(row_date).replace(tzinfo=timezone(self.tz))
        return data_time

    def get_station_data(self, resp):
        row_names = ('Date', 'HOP', 'HGC', 'BSY', 'MEX', 'MTC', 'HEW', 'CLY')

        # опрацювання pdf документу
        stream = StringIO.StringIO(resp.body)

        rsrcmgr = PDFResourceManager()
        retstr = StringIO.StringIO()
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, laparams=laparams)

        process_pdf(rsrcmgr, device, stream)
        device.close()

        doc_str = retstr.getvalue()
        retstr.close()

        # розбиваємо построчно
        row_data = doc_str.split('\n')

        # data_time = row_data[len(row_data)-7]
        # print(data_time)

        first_row = row_data[17:25]
        data = zip(row_names, first_row)

        data_time = self.get_date(data[0][1])
        data = data[1:]

        for st in data:
            station_id = st[0]
            _name = 'PM10_24HR'
            _val = st[1]
            if '*' in _val:
                _val = _val.replace('*', '')

            _tmp_dict = Kind(self.name).get_dict(r_key=_name, r_val=_val)

            station_data = dict()
            if _tmp_dict:
                station_data[_tmp_dict['key']] = _tmp_dict['val']

            # print(station_data)

            if station_data:
                items = AppItem()
                items['scrap_time'] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
                items['data_time'] = data_time
                items['data_value'] = station_data
                items['source'] = 'http://superpit.com.au'
                items['source_id'] = station_id

                yield items

    def parse(self, response):
        for st in self.get_station_data(response):
            yield st
