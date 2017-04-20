#  coding: utf-8
import logging
from os import makedirs
from os.path import exists, dirname
from itertools import chain

import pollution_app.rextension as rextension


class Kind(object):
    instance = None

    def __new__(cls, spider_name):
        if cls.instance is None:
            __path_crawling_root = './'
            __path_crawling_logs = __path_crawling_root + 'crawling_logs/'
            __path_items = __path_crawling_logs + 'items/'

            # directories
            __crawling_logs = dirname(__path_crawling_logs)
            __items = dirname(__path_items)
            # make directories
            if not exists(__crawling_logs):
                makedirs(__crawling_logs)
            if not exists(__items):
                makedirs(__items)

            cls.logger = logging.getLogger('pollution_kind_logger')

            cls.fileHandler = logging.FileHandler(
                __items + '/' + spider_name + '_pollution_kind.log',
                mode='w',
                encoding='utf-8'
            )
            cls.formatter = logging.Formatter('[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
            cls.fileHandler.setFormatter(cls.formatter)
            cls.logger.setLevel(logging.WARNING)
            cls.logger.addHandler(cls.fileHandler)

            cls.spider_name = spider_name
            cls.instance = super(Kind, cls).__new__(cls)

        return cls.instance

    def __init__(self, spider_name):

        self.kind = {
            # show
            (u'SNOWDEPTH',): u'sn_d',
            # precipitation
            (u'Rainfall', u'Rain Fall', u'RAIN', u'Rain', u'RF(mm)'): u'rain',
            (u'PRECIP_TOTAL',): u'prec_total',
            # pres
            (u'Barometric Pressure', u'Bar Pressure', u'Air Pressure', u'Bar pressure', u'Pressure', u'AMBIENTPRESS', u'AP', u'Amb Press', u'P(hPa)'): u'pres',
            (u'VAPOUR_PRESSURE',): u'pres_vap',
            (u'ATM_PRES_1HRAVG',): u'pres_atm_1hr',
            (u'BAR_PRESS_HOUR',): u'pres_bar_1hr',
            (u'FIL_PRESS_25_24HR',): u'pres_fil_25_24hr',
            (u'FIL_PRESS_25',): u'pres_fil_25',
            (u'FIL_PRESS_10',): u'pres_fil_10',
            (u'FIL_PRESS_10_24HR',): u'pres_fil_10_24hr',
            # wind dir
            (u'WD', u'Wind Direction', u'Wind Direction ', u'WindDirection', u'Wind Dir', u'WINDDIRECTION', u'WD(deg)',
             u"Resultant Wind Direction", u"Wind Dir V Deg", u"wd", u"WindDir Deg"): u'wd',
            (u'WDIR_U1MIN',): u'wd_u1min',
            (u'WDIR_SCLR',): u'wd_sclr',
            (u'WDIR_VECT', u'Wind Dir V', u'WDV', u'VWD', u"V Wdir Deg"): u'wd_v',
            (u'WDIR_VECT_BAM',): u'wd_v_bam',
            (u'WDIR_UVEC',): u'wd_u_v',
            # wind speed
            (u'WS', u'Wind Speed', u'Wind Speed ', u'WINDSPEED', u'WS(m/s)', u"Resultant Wind Speed",
             u"Wind Speed V m/s", u"ws", u"WS(m/s) m/s"): u'ws',
            (u'WSPD_SCLR_BAM',): u'ws_sclr_bam',
            (u'WSPD_SCLR',): u'ws_sclr',
            (u'WSPD_3SEC',): u'ws_3sec',
            (u'WSPD_1MIN',): u'ws_1min',
            (u'WindSp mph',): u'ws_m_hr',
            (u'V WSpd mph',): u'ws_v_m_hr',
            (u'WS10min(m/s)',): u'ws_10min',
            (u'Vertical Wind Speed', u'WSPD_VECT', u'Wind Speed V', u'WSV', u'VWS'): u'ws_v',
            # temp
            (u'Temperature', u'TEMP_MEAN', u'TEMP', u'Temp', u'Outdoor Temperature'): u'temp',
            (u'10m Temp °C (10m)',): u'temp_10m',
            (u'2m TempF °F',): u'temp_2m_F',
            (u'TEMP_MEAN_BAM',): u'temp_bam',
            (u'Shelter_T',): u'temp_sh',
            (u'TEMP_INSIDE',): u'temp_ins',
            (u'Ambient Temperature', u'AMBIENTTEMP', u'ATE'): u'temp_amb',
            (u'Rack Temp', u'Rack Temperature'): u'temp_rack',
            # hum
            (u'HUMIDITY', u'HUM'): u'hum',
            (u'Relative Humidity', u'RH', u'Rel Humidity', u"rhum", u"RH %"): u'hum_rel',
            # radiation
            (u'Solar Radiation', u"Solar Rad W/M2", u"SolRad langly"): u'rad_solar',
            (u'RAD_TOTAL',): u'rad_total',
            (u'UV',): u'uv',
            # ozone
            (u'OZONE', u'Ozone', u'OZNE', u'O3', u'o3', u"Ozono", u"Ozone ppb", u"O3 ppm", u"Ozone ppm"): u'o3',
            (u'OZNE_8HR', u'O3 Média 8  h', u"Ozone 3 8 Hour mean", u"O 3 8 Hour mean"): u'o3_8hr',
            (u'Ozone 1-hour  average', u'o3_1hr', u'O3 Média horária'): u'o3_1hr',
            (u'Ozone rolling 4-hour average', u'o3_4hr'): u'o3_4hr',
            # pm
            (u'PM', u'Fine Particles', u'PM Coarse'): u'pm',
            (u'TSP', u'Particles TSP'): u'tsp',
            (u'TSP 24 Hour mean', "Total Particulates 24 Hour mean"): u'tsp_24hr',
            (u'TSP_BAM',): u'tsp_bam',
            (u'TSP_1HR',): u'tsp_1hr',
            # pm25
            (u'PM2.5', u'PM 2.5', u'PM25', u'pm25', u'Particles as PM2.5', u'PM2.5(ug/m^3)', u'Particle PM2.5',
             u'pm2_5', u"MP 2,5", u"1-Hour PM 2.5", u"PM-2.5 (Local Conditions)", u"PM2.5 (NEPH) ug/m3 (L)"): u'pm25',
            (u'PM25_24HR', u'Particles rolling 24-hour average (PM25)', u'MP2.5 Média 24   h', u"PM 2.5 24 Hour mean",
             u"24-Hour PM 2.5"): u'pm25_24hr',
            (u'PM25_5030i_24HR',): u'pm25_5030i_24hr',
            (u'PM25_5030i', u'iPM2.5'): u'pm25_5030i',
            (u'PM2.5 (Sharp 5030)',): u'pm25_sharp_5030',
            (u'PM25_SHARP_DUST',): u'pm25_sharp_dust',
            (u'PM25_SHARP_DUST_24HR',): u'pm25_sharp_dust_24hr',
            (u'BAM25', u'PM2_5_BAM', u'BPM2.5'): u'pm25_bam',
            (u'BAM25_24HR',): u'pm25_bam_24hr',
            (u'PM2_5_RUN_AVG',): u'pm25_avg',
            (u'MP2.5 Média horária',): u'pm25_1hr',
            (u'Non-volatile PM 2.5 24 Hour mean',): u'pm10_non_volatile_24hr',
            (u'Volatile PM 2.5 24 Hour mean', u"Volatile PM2.5 2.5 24 Hour mean"): u'pm10_volatile_24hr',
            # pm 10
            (u'PM10', u'PM 10', u'pm10', u'Particles as PM10', u'Particles (PM10)', u'PM10(ug/m^3)', u'Particle PM10',
             u'PM10 (FH62)', u"MP 10", u"PM-10 (Standard Conditions)"): u'pm10',
            (u'PM10_24HR', u'Particles rolling 24-hour average (PM10)', u'MP10 Média 24   h', u"PM 10 24 Hour mean"): u'pm10_24hr',
            (u'PM10_BAM_24HR', ): u'pm10_bam_24hr',
            (u'PM10 (Sharp 5030)',): u'pm10_sharp_5030',
            (u'PM10_BAM',): u'pm10_bam',
            (u'PM10_RUN_AVG',): u'pm10_avg',
            (u'MP10 Média horária',): u'pm10_1hr',
            (u'Non-volatile PM 10 24 Hour mean',): u'pm10_non_volatile_24hr',
            (u'Volatile PM 10 24 Hour mean',): u'pm10_volatile_24hr',
            # pm1
            (u'PM 1 24 Hour mean', u"PM1 Particulates 1 24 Hour mean"): u'pm1_24hr',
            # visibility
            (u'VISI', u'Visibility Reduction', u'Visibility'): u'visi',
            (u'Visibility 1-hour  average',): u'visi_1hr',
            # grimm
            (u'GRIMM10',): u'grimm_10',
            (u'GRIMM01',): u'grimm_01',
            (u'GRIMM25',): u'grimm_25',
            (u'GRIMM25_24HR',): u'grimm_25_24hr',
            (u'GRIMM01_24HR',): u'grimm_01_24hr',
            (u'GRIMM10_24HR',): u'grimm_10_24hr',
            # co co2
            (u'Carbon Monoxide', u'C--O', u'CO', u'CO', u'co', u'Carbon monoxide', u'CO Média horária', u"CO ppm"): u'co',
            (u'C--O_8HR', u'Carbon monoxide rolling 8-hour  average', u'CO Média 8  h', u"Carbon monoxide 8 Hour mean",
             u"CO 8 Hour mean"): u'co_8hr',
            # no no2
            (u'NO2', u'Nitrogen Dioxide', u'Nitrogen dioxide', u'NO-2', u'no2', u"NO 2 Hourly mean",
             u"Nitrogen dioxide 2 hourly mean", u'Nitrogen dioxide 1-hour  average',
             u'NO2 Média Horária', u"Nitrogen dioxide 2 Hourly mean", u"NO2_ppm ppm", u"NO2 ppb", u"NO2 ppm"): u'no2',
            (u'Nitric Oxide', u'N--O', u'NO', u"Nitric oxide Hourly mean", u"NO Hourly mean",
             u"Nitric oxide hourly mean", u"NO ppb", u"NO ppm"): u'no',
            (u'NOx', u'NOX', u"Nitrogen oxides as nitrogen dioxide 2 Hourly mean", u"NO X Hourly mean",
             u"Nitrogen oxides as nitrogen dioxide x Hourly mean", u"Nitrogen oxides as nitrogen dioxide x hourly mean",
             u"NOX ppb"): u'no_x',
            (u'NOY', u'NOy'): u'no_y',
            (u'NO-2_24HR',): u'no2_24hr',
            (u'N--O_24HR',): u'no_24hr',
            # so so2
            (u'Sulphur Dioxide', u'Sulfur Dioxide', u'SO-2', u'SO2', u'so2', u'Sulfur dioxide', u"Dióxido de azufre",
             u"SO2_ppm ppm"): u'so2',
            (u'SO-2_24HR', u'SO2 Média 24   h'): u'so2_24hr',
            (u'SO2H_ppm ppm'): u'so2h',
            (u'Sulfur dioxide 1-hour  average', u'SO2 Média horária'): u'so2_1hr',
            (u"Sulphur dioxide 2 15 Minute mean", u"SO 2 15 Minute mean"): u"so2_15min",
            # xyl
            (u'Xylene',): u'xyl',
            (u'P Xylene',): u'p_xyl',
            (u'O Xylene',): u'o_xyl',
            (u'MP Xylene', u'M & P Xylene'): u'mp_xyl',
            # h2s
            (u'H2-S', u'H2S', u"H2SH_PPM ppm"): u'h2s',
            (u'H2-S_24HR',): u'h2s_24hr',
            # hf
            (u'H--F',): u'hf',
            (u'H--F_24HR',): u'hf_24hr',
            # trs
            (u'Total Reduced Sulphur', u'TRS'): u'trs',
            (u'TRS Média Horária',): u'trs_1hr',
            #
            (u'Non Methane HydroCarbon', u'NMHC', u'Non Methane Hydrocarbon'): u'nmhc',
            (u'Total Hydrocarbon',): u'total_hydca',
            (u'NMOG',): u'nmog',
            (u'Ni',): u'ni',
            (u'Oxides of Nitrogen',): u'n2o',
            (u'Ethyl Benzene',): u'et_ben',
            # toluene
            (u'Toluene', u'Toulene'): u'ch3',
            (u'TOLUENO Média Horária',): u'ch3_1hr',
            #
            (u'THC',): u'thc',
            (u'BC',): u'bc',
            (u'Methane', u'CH4'): u'ch4',
            # benzen
            (u'Benzene',): u'c6h6',
            (u'BENZENO Média Horária',): u'c6h6_1hr',
            #
            (u'Ammonia',): u'nh3',
            (u'Pb', u'pb', u'lead'): u'pb',
            (u'Arsenic', u'As', u'as'): u'as',
            (u'Iron', u'Fe', u'fe'): u'fe',
            (u'Antimony', u'Sb', u'sb'): u'sb',
            (u'SD_SDIR',): u'sd_sdir',
            (u'PLUVIO_STATUS',): u'pluvito_status',
            (u'BAM_FLOW',): u'bam_flow',
            (u'TIME_3SEC',): u'time_3sec',
            # sigm
            (u'SIGM_ALLP',): u'sigm_all_p',
            (u'SIGM_ALL',): u'sigm_all',
            (u'AQI', u'AQHI', u'aqi', u'Station Index'): u'aqi',
        }

        self.unknown = (
            # manitoba
            'SO4 Cycle',
            'PM 2.5s',
            'Wind Dir S',
            'PM 2.5t',
            'NH3',
            'PM10t',
            'Wind Speed S',
            # hamilton
            'Low Alarm',
            'WChill',
            'WSMx',
            'BP',
            'WS1',
            'ATMn_T',
            'WS3',
            'WSMx_T',
            'WD3',
            'TEM2',
            'TEM3',
            'TEM1',
            'ATEM',
            'HumDx',
            'AT',
            'ATMx',
            'ATMx_T',
            'WDSD',
            'WD1',
            'WD2',
            'WS2',
            'DT2',
            'DT1',
            'ATMn',
            # new_foundland
            'STAT_NUM',
            # new brunswik
            'Fine Particulate Matter',
            'Tout(degC)',
            'Tin(degC)',
            'RHout(%)',
            'RHin(%)',
            'RFRate(mm/hr)',
            # au_victoria
            'BSP',
            'SIG60',
            'TDBT',
            'BHUM',
            'API',
            'DBT',
            # japan
            'OX',
            'SPM',
            'SP',
            '',
        )

    @staticmethod
    def validate(val):
        """try to make float value from input and replace , to ."""
        try:
            if isinstance(val, float):
                return val
            elif isinstance(val, int):
                return float(val)
            elif val is not None:
                if ',' in val:
                    val = val.replace(',', '.')
                elif u',' in val:
                    val = val.replace(u',', u'.')
                _val = float(val)
            else:
                raise ValueError
        except ValueError:
            _val = None

        return _val

    def get_key(self, r_key):
        """
        проганяє сирий ключ через словник значень
        :param r_key: показник забруднення
        :return: стандартизований ключ
        """
        for kind in self.kind:
            if r_key in kind:
                key = self.kind.get(kind)
                return key

    def get_dict(self, r_key, r_val, source_name=None):
        """
        повертає значення словника в тому разі якщо
        такий показник представлений в списку класу
        """
        val = self.validate(r_val)
        if val is not None:
            if r_key in rextension.unbend(self.kind.keys()):
                for kind in self.kind:
                    if r_key in kind:
                        key = self.kind.get(kind)
                        _dict = {u'key': key, u'val': val}
                        return _dict
            elif r_key not in rextension.unbend(self.kind.keys()):
                self.logger.warning(u'There is no such pollution name in kind list : |{0}| [{1}]'.format(
                    r_key,
                    source_name
                ))
            else:
                self.logger.warning(u'Something goes wrong with keys: |{0}| [{1}]'.format(
                    r_key,
                    source_name
                ))

        else:
            self.logger.warning(
                u'''Input integer value is NONE or invalid:
                Input value: {0}
                Value after validation: {1}
                Row key: {2}
                Source name: {3}
                '''.format(
                    r_val,
                    val,
                    r_key,
                    source_name
                )
            )
            return None

    def convert(self):
        pass
