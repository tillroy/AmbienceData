# coding: utf-8
from scrapy import Spider, Request
from pollution_app.pollution import Kind
from pollution_app import rextension
from pollution_app.items import AppItem
from pollution_app.settings import SCRAPER_TIMEZONE
from datetime import datetime
from pytz import timezone
from dateutil.parser import parse


class CetesbSpider(Spider):
    name = u"br_cetesb"
    tz = u"Brazil/East"
    source = u"http://www.cetesb.sp.gov.br"
    values_list = (
        u"52/Americana",
        u"72/Araçatuba",
        u"71/Araraquara",
        u"73/Bauru",
        u"42/Campinas-Centro",
        u"41/Campinas-Taquaral",
        u"43/Campinas-V.União",
        u"37/Capão Redondo",
        u"28/Carapicuiba",
        u"81/Catanduva",
        u"10/Cerqueira César",
        u"31/Cid.Universitária-USP-Ipen",
        u"8/Congonhas",
        u"24/Cubatão-Centro",
        u"30/Cubatão-Vale do Mogi",
        u"25/Cubatão-Vila Parisi",
        u"15/Diadema",
        u"35/Guarulhos-Paço Municipal",
        u"40/Guarulhos-Pimentas",
        u"5/Ibirapuera",
        u"34/Interlagos",
        u"33/Itaim Paulista",
        u"50/Itaquera",
        u"54/Jacareí",
        u"75/Jaú",
        u"74/Jundiaí",
        u"64/Limeira",
        u"36/Marg.Tietê-Ponte dos Remédios",
        u"76/Marília",
        u"22/Mauá",
        u"3/Mooca",
        u"6/Nossa Senhora do Ó",
        u"17/Osasco",
        u"29/Parelheiros",
        u"1/Parque D.Pedro II",
        u"44/Paulínia",
        u"45/Paulínia - Sul",
        u"27/Pinheiros",
        u"77/Piracicaba",
        u"78/Presidente Prudente",
        u"79/Ribeirão Preto",
        u"18/S.André-Capuava",
        u"32/S.André-Paço Municipal",
        u"38/S.Bernardo-Centro",
        u"19/S.Bernardo-Paulicéia",
        u"55/S.José Campos",
        u"56/S.José Campos-Jd.Satélite",
        u"57/S.José Campos-Vista Verde",
        u"53/Santa Gertrudes",
        u"2/Santana",
        u"16/Santo Amaro",
        u"82/Santos",
        u"83/Santos-Ponta da Praia",
        u"7/São Caetano do Sul",
        u"80/São José do Rio Preto",
        u"51/Sorocaba",
        u"20/Taboão da Serra",
        u"94/Tatuí",
        u"58/Taubaté"
    )

    custom_settings = {
        u"DOWNLOADER_MIDDLEWARES": {u"pollution_app.middlewares.CetesbDownloader": 400}
    }

    def start_requests(self):
        """generate different request to the same url"""
        # FIXME
        # for val in ("17/Osasco", "75/Jaú"):
        for val in self.values_list:
            yield Request(
                url=u"http://sistemasinter.cetesb.sp.gov.br/Ar/php/ar_dados_horarios.php",
                dont_filter=True,
                meta={u"select": val}
            )

    def get_date(self, date, hour):
        date = str(date)
        hour = str(hour)
        data_time = date + u" " + hour
        data_time = parse(data_time).replace(tzinfo=timezone(self.tz))
        return data_time

    def parse(self, response):
        station_id = response.meta[u"select"]
        station_id = station_id.split(u"/")
        try:
            station_id = station_id[0]
        except IndexError:
            station_id = None

        id_and_date = response.xpath(u"/html/body/center/table[3]/tbody/tr/td[3]/p[1]/text()").extract_first()
        id_and_date = id_and_date.split(u" - ")
        try:
            st_id = id_and_date[0]
            date = id_and_date[1]
        except ImportError:
            st_id = None
            date = None

        cols = response.xpath(u"/html/body/center/table[3]/tbody/tr/td[3]/table[1]/tbody/tr/td")
        names = list()
        for col in cols:
            poll_name = col.xpath(u"table/tbody/tr[1]/td[1]/b/text()").extract_first()
            if poll_name is None:
                poll_name = col.xpath(u"table/tbody/tr[1]/td[1]/p/strong/text()").extract_first()
            poll_name = poll_name.encode(u"utf-8")
            # FIXME
            # print(type(poll_name), repr(poll_name), repr(poll_name.encode("utf-8")))

            poll_kinds = col.xpath(u"table/tbody/tr[2]/td")
            part_names = list()
            for row_poll_kind in poll_kinds:
                poll_kind = row_poll_kind.xpath(u"p/text()").re(u"\s*(.+)\s*")
                # poll_kind = u" ".join(poll_kind)
                # # FIXME
                # print(type(poll_kind), repr(poll_kind), repr(poll_kind))

                if len(poll_kind) == 0:
                    poll_kind = row_poll_kind.xpath(u"text()").re(u"\s*(.+)\s*")
                full_name = u" ".join([poll_name, u" ".join(poll_kind)])

                part_names.append(full_name)
            names.extend(part_names)
        names = names[1:]
        # print(names)

        parts = response.xpath(u"/html/body/center/table[3]/tbody/tr/td[3]/table[1]/tbody/tr/td")
        row_tbl = list()
        for part in parts:
            _part = list()
            _rows = part.xpath(u"table/tbody/tr")
            for _row in _rows[2:]:
                full_vals = list()
                _cols = _row.xpath(u"td")
                for _col in _cols:
                    _val = _col.xpath(u"text()").extract_first()
                    if _val is None:
                        _val = _col.xpath(u"font/text()").extract_first()
                    # FIXME
                    # print(type(_val), repr(_val), repr(_val.encode("utf-8")))

                    full_vals.append(_val.encode(u"utf-8"))
                _part.append(full_vals)
            row_tbl.append(_part)

        row_tbl = zip(*row_tbl)

        tbl = map(rextension.unbend, row_tbl)
        tbl = [val for val in tbl if len(val) - val.count(u"--") != 1]

        try:
            cur_val = tbl[len(tbl) - 1]
            hour = cur_val[0]
            data_time = self.get_date(date, hour)

            cur_val = cur_val[1:]
            data = zip(names, cur_val)
        except IndexError:
            data = None
            data_time = None

        st_data = dict()
        if data is not None:
            for val in data:
                _name = val[0]
                _val = val[1]
                _tmp_dict = Kind(spider_name=self.name).get_dict(r_key=_name, r_val=_val, source_name=st_id)
                if _tmp_dict:
                    st_data[_tmp_dict[u"key"]] = _tmp_dict[u"val"]

        if st_data:
            items = AppItem()
            items[u"scrap_time"] = datetime.now(tz=timezone(SCRAPER_TIMEZONE))
            items[u"data_time"] = data_time
            items[u"data_value"] = st_data
            items[u"source"] = self.source
            items[u"source_id"] = station_id

            return items
