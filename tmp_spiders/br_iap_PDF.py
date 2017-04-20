# coding: utf-8
from scrapy import Spider, Request
from datetime import datetime
# pdf
import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, process_pdf

class IapSpider(Spider):
    name = 'br_iap'
    tz = 'Etc/GMT-3'

    def start_requests(self):
        # stations = ('BOQ', 'PAR', 'STC', 'CIC', 'ASS', 'CSN', 'RPR', 'UEG')
        stations = ('BOQ',)
        month = str(datetime.now().month)
        if len(month) == 1:
            month = '0' + month
        else:
            month = month
        month = '08'

        year = str(datetime.now().year)
        href = 'http://www.iap.pr.gov.br/arquivos/File/Monitoramento/qualidade_do_ar_laptec/IQA_'

        for station_id in stations:
            url = href + station_id + '_' + month + '_' + year + '.pdf'
            # print(url)
            yield Request(
                url=url,
                meta={'station_id': station_id}
            )

    def get_BOQ(self, row_data):
        # INFO mpts
        row_mpts = row_data[12:56]
        mpts = list()
        for el in row_mpts:
            if el != '':
                if el != ' ':
                    mpts.append(el)
        mpts = mpts[len(mpts)-1]
        print('MPTS', mpts)




        # INFO no2
        row_no2 = row_data[331:375]
        no2 = list()
        for el in row_no2:
            if el != '':
                if el != ' ':
                    no2.append(el)
        no2 = no2[len(no2)-1]
        print('NO2', no2)

        # INFO o3
        row_o3 = row_data[375:419]
        o3 = list()
        for el in row_o3:
            if el != '':
                if el != ' ':
                    o3.append(el)
        o3 = o3[len(o3)-1]
        print('O3', o3)

        # INFO co
        row_co = row_data[419:463]
        co = list()
        for el in row_co:
            if el != '':
                if el != ' ':
                    co.append(el)
        co = co[len(co)-1]
        print('CO', co)

        # INFO pm10
        row_pm10 = row_data[463:509]
        pm10 = list()
        for el in row_pm10:
            if el != '':
                if el != ' ':
                    pm10.append(el)
        pm10 = pm10[len(pm10)-1]
        print('PM10', pm10)

        # INFO so2
        row_so2 = row_data[509:555]
        so2 = list()
        for el in row_so2:
            if el != '':
                if el != ' ':
                    so2.append(el)
        so2 = so2[len(so2)-1]
        print('SO2', so2)

        # INFO ?
        row_so2 = row_data[463:463]





        # INFO temp/umid/no2
        row_so2 = row_data[56:100]

    def print_row_data(self, row_data):
        ind = 0
        open('BOQ.txt', 'w').close()
        for el in row_data:
            f = open('BOQ.txt', 'a')
            string = str(ind) + ')  ' + str(el) + '\n'
            f.write(string)
            ind += 1

    def get_station_data(self, resp):
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
        # FIXME
        self.get_BOQ(row_data)
        # self.print_row_data(row_data)

    def parse(self, response):
        self.get_station_data(response)

