#  coding: utf-8
import logging
from os import makedirs
from os.path import exists, dirname
from itertools import chain


class Feature(object):
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
            cls.instance = super(Feature, cls).__new__(cls)

        return cls.instance

    def __init__(self, spider_name):
        # variable for global data about URL of page where is a problem
        self.source = None

        self.raw_name = None
        self.raw_value = None
        self.raw_units = None

        # converted and vakidated
        self.valid_value = None
        self.valid_name = None
        self.valid_units = None

        self.kind = {
            # show
            (u'SNOWDEPTH', u"snow"): u'sn_d',
            #sky
            (u'sky',): u'sky',
            # precipitation
            (
                u'Rainfall', u'Rain Fall', u'RAIN', u'Rain', u'RF(mm)', u"Опади", u"Precipitation", u"precipitation",
                u"RAINFALL", u"hly-precip", u"Rainfall, total for hour"
            ): u'rain',
            (u'PRECIP_TOTAL', u"Total Precip."): u'prec_total',
            # pres
            (
                u'Barometric Pressure', u'Bar Pressure', u'Air Pressure', u'Bar pressure', u'Pressure', u'AMBIENTPRESS',
                u'AP', u'Amb Press', u'P(hPa)', u"BP", u"Press Amb", u"Атмосферний тиск", u"BP (mBar)", u"BARPR",
                u"PRES", u"mslp", u'VAPOUR_PRESSURE', u"hly-vap-pres",
             ): u'pres',
            (u'ATM_PRES_1HRAVG',): u'pres_atm_1hr',
            (u'BAR_PRESS_HOUR',): u'pres_bar_1hr',
            (u'FIL_PRESS_25_24HR',): u'pres_fil_25_24hr',
            (u'FIL_PRESS_25',): u'pres_fil_25',
            (u'FIL_PRESS_10',): u'pres_fil_10',
            (u'FIL_PRESS_10_24HR',): u'pres_fil_10_24hr',
            # wind dir
            (
                u'WD', u'Wind Direction', u'Wind Direction ', u'WindDirection', u'Wind Dir', u'WINDDIRECTION',
                u'WD(deg)', u"Resultant Wind Direction", u"WindDir", u"wd", u"WindDir Deg", u"WDIR", u"Wind Dir S",
                u"Напрямок вітру", u"WDR US", u"WDR", u"WD-R", u"hly-wind-dir", u"wdir", u"Vector Wind Direction",
                u"wxWDIR"
            ): u'wd',
            (u'WDIR_U1MIN',): u'wd_u1min',
            (u'WDIR_SCLR',): u'wd_sclr',
            (
                u'WDIR_VECT', u'Wind Dir V', u'WDV', u'VWD', u"V Wdir", u"VWDR",
                u"Vertical Wind Direction"
            ): u'wd_v',
            (u'WDIR_VECT_BAM',): u'wd_v_bam',
            (u'WDIR_UVEC',): u'wd_u_v',
            # wind speed
            (
                u'WS', u'Wind Speed', u'Wind Speed ', u'WINDSPEED', u"Wind Speed", u"ws",
                u"WSPD", u"Wind Speed S", u"Wind Spd S", u"WindSp", u"Швидкість вітру", u"WS (m/s)", u"WSP US",
                u"SWINSPD_mph", u"WSP", u"WS-R", u"Wind", u"hly-wind-spd", u"Vector Wind Speed", u"wxWSPD"
            ): u'ws',
            (u'WSPD_SCLR_BAM',): u'ws_sclr_bam',
            (u'WSPD_SCLR',): u'ws_sclr',
            (u'WSPD_3SEC',): u'ws_3sec',
            (u'WSPD_1MIN',): u'ws_1min',
            (u'WS10min(m/s)',): u'ws_10min',
            (u'Wind Max',): u'ws_max',
            (
                u'Vertical Wind Speed', u'WSPD_VECT', u'Wind Speed V', u'WSV', u'VWS', u"V WSpd",
                u"Wind Spd V", u"VWSP"
            ): u'ws_v',
            # temp
            (
                u'Temperature', u'TEMP_MEAN', u'TEMP', u'Temp', u'Outdoor Temperature', u'Ambient Temperature',
                u'AMBIENTTEMP', u'ATE', u"Temp Amb C", u"Temp Amb", u"Температура повітря", u"RTEMP", u"OTEMP",
                u"Air Temperature", u"hly-air-tmp", u"temp", u"Ambient Temperature (degrees F)"
             ): u'temp',
            (u"Температура води",): u'temp_wt',
            (u"hly-soil-tmp",): u'temp_soil',
            (u'10m Temp °C (10m)', u"Temp_10", u"Temp Amb 10m", u"10m Temp"): u'temp_10m',
            (u'2m TempF', u"Temp_2m"): u'temp_2m',
            (u'TEMP_MEAN_BAM',): u'temp_bam',
            (u'Shelter_T',): u'temp_sh',
            (u'TEMP_INSIDE', u"ITEMP",): u'temp_ins',
            (u'Rack Temp', u'Rack Temperature'): u'temp_rack',
            (
                u'Температура точки роси', u"Temp dew point", u"hly-dew-pnt", u"dewpoint",
                u"Dew Point Temperature"): u'temp_dew_p',
            # hum
            (
                u'HUMIDITY', u'HUM', u"humidity", u'Relative Humidity', u'RH', u'Rel Humidity',
                u"rhum", u"RH %", u"RHUM", u"hly-rel-hum", u"wxRH"
            ): u'hum',

            # radiation
            (
                u'Solar Radiation', u"SolRad", u"SOLAR", u"Solar Rad", u"SOLRA", u"SRAD",
                u"hly-sol-rad", u"Ultraviolet Radiation"): u'rad_solar',
            (u'RAD_TOTAL',): u'rad_total',
            (u'UV',): u'uv',
            (u'hly-net-rad',): u'rad_net',
            # ozone
            (
                u'OZONE', u'Ozone', u'OZNE', u'O3', u'o3', u"Ozono", u"Ozone ppb", u"O3 ppm", u"Ozone ppm", u"O3-3",
                u"OZONE_ppm", u"ozone", u"Ozone PPM", u"Ozone PPB"
            ): u'o3',
            (
                u'OZNE_8HR', u'O3 Média 8  h', u"Ozone 3 8 Hour mean", u"O 3 8 Hour mean",
                u"Ozone 8 Hour Rolling Average", u"O3_8HR"
            ): u'o3_8hr',
            (u'Ozone 1-hour  average', u'o3_1hr', u'O3 Média horária', u"Ozone 1 hour", u"Ozone 1 Hour Average"): u'o3_1hr',
            (u'Ozone rolling 4-hour average', u'o3_4hr'): u'o3_4hr',
            # pm
            (u'PM', u'Fine Particles', u'PM Coarse', u"PM-Coarse", u"pm", u"PMcrs"): u"pm",
            (u'TSP', u'Particles TSP'): u'tsp',
            (u'TSP 24 Hour mean', u"Total Particulates 24 Hour mean"): u'tsp_24hr',
            (u'TSP_BAM',): u'tsp_bam',
            (u'TSP_1HR',): u'tsp_1hr',
            # pm25
            (
                u'PM2.5', u'PM 2.5', u'PM25', u'pm25', u'Particles as PM2.5', u'PM2.5(ug/m^3)', u'Particle PM2.5',
                u'pm2_5', u"MP 2,5", u"1-Hour PM 2.5", u"PM-2.5 (Local Conditions)", u"PM2.5 (NEPH)",
                u"PM-2.5", u"PM2.5 (TEOM)", u"NPM25", u"TPM25", u"PM2.5_WEB", u"PM25C", u"PM25FEM", U"PM25R",
                u"FEM_TPM25", u"Fine Particles (PM2.5)", u"DF-PM25 FEM", u"F-PM25 FEM"
            ): u'pm25',
            (u'PM25_24HR', u'Particles rolling 24-hour average (PM25)', u'MP2.5 Média 24   h', u"PM 2.5 24 Hour mean",
             u"24-Hour PM 2.5", u"Particulate Matter PM2.5 24 Hour Rolling Average"): u'pm25_24hr',
            (u'PM25_5030i_24HR',): u'pm25_5030i_24hr',
            (u'PM25_5030i', u'iPM2.5'): u'pm25_5030i',
            (u'PM2.5 (Sharp 5030)',): u'pm25_sharp_5030',
            (u'PM25_SHARP_DUST',): u'pm25_sharp_dust',
            (u'PM25_SHARP_DUST_24HR',): u'pm25_sharp_dust_24hr',
            (u'BAM25', u'PM2_5_BAM', u'BPM2.5', u"PM 2.5 BAM", u"BAM_PM25"): u'pm25_bam',
            (u'BAM25_24HR',): u'pm25_bam_24hr',
            (u'PM2_5_RUN_AVG',): u'pm25_avg',
            (u'MP2.5 Média horária', u"PM25 Automated Method 1hr sample at local conditions"): u'pm25_1hr',
            (u'Non-volatile PM 2.5 24 Hour mean',): u'pm10_non_volatile_24hr',
            (u'Volatile PM 2.5 24 Hour mean', u"Volatile PM2.5 2.5 24 Hour mean"): u'pm10_volatile_24hr',
            # pm 10
            (
                u'PM10', u'PM 10', u'pm10', u'Particles as PM10', u'Particles (PM10)', u'PM10(ug/m^3)',
                u'Particle PM10', u'PM10 (FH62)', u"MP 10", u"PM-10 (Standard Conditions)", u"PM10-NEW",
                u"PM10 (s-TEOM)", u"TPM10", u"NPM10", u"PM10_Teom", u"DF-PM10", u"PM10_teom",
                u"PM10 Automated Method Measured at Local Conditions", u"PM-10 (Local Conditions)"
            ): u'pm10',
            (
                u'PM10_24HR', u'Particles rolling 24-hour average (PM10)', u'MP10 Média 24   h',
                u"PM 10 24 Hour mean", u"Particulate Matter PM10 24 Hour Rolling Average"
            ): u'pm10_24hr',
            (u'PM10_BAM_24HR', ): u'pm10_bam_24hr',
            (u'PM10 (Sharp 5030)',): u'pm10_sharp_5030',
            (u'PM10_BAM', u"PM 10 BAM"): u'pm10_bam',
            (u'PM10_RUN_AVG',): u'pm10_avg',
            (u'MP10 Média horária', u"PM10HR "): u'pm10_1hr',
            (u'Non-volatile PM 10 24 Hour mean',): u'pm10_non_volatile_24hr',
            (u'Volatile PM 10 24 Hour mean',): u'pm10_volatile_24hr',
            # pm1
            (u'PM 1 24 Hour mean', u"PM1 Particulates 1 24 Hour mean"): u'pm1_24hr',
            # visibility
            (u'VISI', u'Visibility Reduction', u'Visibility', u"Visual Range"): u'visi',
            (u'Visibility 1-hour  average',): u'visi_1hr',
            # grimm
            (u'GRIMM10',): u'grimm_10',
            (u'GRIMM01',): u'grimm_01',
            (u'GRIMM25',): u'grimm_25',
            (u'GRIMM25_24HR',): u'grimm_25_24hr',
            (u'GRIMM01_24HR',): u'grimm_01_24hr',
            (u'GRIMM10_24HR',): u'grimm_10_24hr',
            # co co2
            (
                u'Carbon Monoxide', u'C--O', u'CO', u'CO', u'co', u'Carbon monoxide',
                u'CO Média horária', u"Trace_CO", u"Trace CO", u"CO-Tr", u"Carbon Monoxide (CO)",
                u"Carbon Dioxide"
            ): u'co',
            (
                u'C--O_8HR', u'Carbon monoxide rolling 8-hour  average', u'CO Média 8  h',
                u"Carbon monoxide 8 Hour mean", u"CO 8 Hour mean", u"CO8HR PPB"
            ): u'co_8hr',
            # no no2
            (
                u'NO2', u'Nitrogen Dioxide', u'Nitrogen dioxide', u'NO-2', u'no2', u"NO2ppb", u"NO2ppm", u"NO2_ppm",
                u"NO2-3", u"NO2T", u"Nitrogen Dioxide (NO2)", u"NO2 PPB"
             ): u'no2',
            (u'NO2 Média Horária', u'Nitrogen dioxide 1-hour  average', u"Nitrogen Dioxide 1 Hour Average"): u"no2_1hr",
            (u"Nitrogen dioxide 2 hourly mean", u"Nitrogen dioxide 2 Hourly mean", u"NO 2 Hourly mean",): u"no2_2hr",
            (u'Nitric Oxide', u'N--O', u'NO', u"Nitric oxide Hourly mean", u"NO Hourly mean",
             u"Nitric oxide hourly mean", u"NOppb", u"NOppm", u"NO_Trace", u"Trace NO", u"NO Trace"): u'no',
            (u'NOx', u'NOX', u"Nitrogen oxides as nitrogen dioxide 2temp_wt Hourly mean", u"NO X Hourly mean",
             u"Nitrogen oxides as nitrogen dioxide x Hourly mean", u"Nitrogen oxides as nitrogen dioxide x hourly mean",
             u"NOXppb", u"NOXppm"): u'no_x',
            (u'NOY', u'NOy', u"Trace NOy", u"NOy-N"): u'no_y',
            (u'NOz',): u'no_z',
            (u'NO-2_24HR',): u'no2_24hr',
            (u'N--O_24HR',): u'no_24hr',
            # so so2
            (
                u'Sulphur Dioxide', u'Sulfur Dioxide', u'SO-2', u'SO2', u'so2', u'Sulfur dioxide', u"Dióxido de azufre",
                u"SO2_ppm", u"SO2ppb", u"SulfurDioxide", u"Trace_SO2", u"Trace SO2", u"SO2-Tr", u"SO2-3",  u"SO2T",
                u"Sulfur Dioxide (SO2)", u"SO2 (PPB)", u"Hourly value of highest 5-minute SO2"
             ): u'so2',
            (u'SO-2_24HR', u'SO2 Média 24   h'): u'so2_24hr',
            (u'SO2H',): u'so2h',
            (u'Sulfur dioxide 1-hour  average', u'SO2 Média horária', u"SO2H_ppm"): u'so2_1hr',
            (u"Sulphur dioxide 2 15 Minute mean", u"SO 2 15 Minute mean"): u"so2_15min",
            # xyl
            (u'Xylene',): u'xyl',
            (u'P Xylene',): u'p_xyl',
            (u'O Xylene',): u'o_xyl',
            (u'MP Xylene', u'M & P Xylene'): u'mp_xyl',
            # h2s
            (u'H2-S', u'H2S', u"H2SH_PPM ppm"): u'h2s',
            (u'H2-S_24HR',): u'h2s_24hr',
            (u'H2SH_PPM',): u'h2s_1hr',
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
            (u'Methane', u'CH4', u"METHANE"): u'ch4',
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
            (u'Sigma',): u'sigma',
            (u'SIGM_ALLP',): u'sigma_all_p',
            (u'SIGM_ALL',): u'sigma_all',
            (u'AQI', u'AQHI', u'aqi', u'Station Index'): u'aqi',
            # AGRO
            (u'hly-eto',): u'eto',
            (u'hly-asce-eto',): u'eto_asce',
            (u'hly-asce-etr',): u'etr_asce',
        }

        self.default_unit = {
            # rain
            (u"rain", u"prec_total", u"eto", u"eto_asce", u"etr_asce"): u"mm",
            # wind
            (u"wd", u"wd_v", u"sigma"): u"deg",
            (u"ws", u"ws_max", u"ws_v"): u"ms",
            # pressure
            (u"pres",): u"kpa",

            (u"visi",): u"km",

            (u"co", u"co2", u"co_8hr", u"h2s_1hr", u"ch4", u"h2s"): u"ppm",
            (u"no", u"no2", u"no2_1hr", u"no_x", u"so2", u"o3", u"o3_1hr", u"o3_8hr", u"ozone", u"no_y", u"n2o", u"so2_1hr"): u"ppb",
            (u"hum_rel", u"hum", u"sky"): u"percent",
            (u"temp", u"temp_10m", u"temp_2m", u"temp_dew_p", u"temp_wt", u"temp_ins", u"temp_soil"): u"degc",

            (u"rad_solar", u"rad_net"): u"ly",
            (u"pm25", u"pm10", u"pm10_24hr", u"pm25_bam", u"pm25_1hr", u"pm10_bam", u"pm", u"pm25_24hr"): u"ug_m3",
        }

        self.current_units = {
            (u"ppm", u"PPM", u"(PPM)", u"Parts per million", u"parts per million"): u"ppm",
            (u"ppb", u"PPB", u"(PPB)", u"Parts per billion", u"parts per billion"): u"ppb",
            (u"in Hg", u"inHg", u"in HG", u"(INHG)", u"IN-HG", u"in. Hg.", u"Inches (mercury)"): u"inhg",
            (u"mmHg",): u"mmhg",
            (u"in", u"(INCHES)", u"inches", u"(in)", ): u"in",
            (u"mbar", u"mb", u"(mBars)", u"millibars"): u"mbar",
            (u"gpa",): u"gpa",
            (
                u"Deg", u"deg", u"(DEG)", u"DEGREES", u"(°)", u"DEG", u"Degrees Compass",
                u"degrees compass"
            ): u"deg",
            (u"mph", u"MPH", u"(MPH)", u"miles per hour"): u"mph",
            (u"Miles",): u"ml",
            (u"Km", u"invK"): u"km",
            (u"mm", u"Millimeters (rainfall)", u"millimeters"): u"mm",
            (u"m/s", u"ms", u"M/SEC", u"Meters/second", u"meters per second"): u"ms",
            (u"w/m2", u"(W/M2)"): u"wm2",
            (
                u"DegF", u"Deg F", u"degF", u"degf", u"F o", u"DEG-F", u"Deg. F.", u"(F)", u"Degrees Fahrenheit",
                u"DEGF", u"degrees Fahrenheit"
            ): u"degf",
            (
                u"DegC", u"C", u"C°", u"degc", u"(DEGC)", u"Degrees Centigrade", u"degrees C",
            ): u"degc",
            (
                u"%", u"Percent", u"(PERCENT)", u"% RH", u"(%)", u"Percent relative humidity",
                u"PERCENT", u"percent relative humidity"
            ): u"percent",
            (u"Langleys", u"langly", u"LANG/MIN", u"(Ly/day)", u"langleys per minute"): u"ly",
            (
                u"ug/m3", u"µg/m3", u"ug/m3(L)", u"ug/m3L", u"ug/m^3", u"µg/m", u"µg/m ", u"ug/m3 (S)", u"ug/m3 (L)",
                u"ug/m^3 (s)", u"ug/m3S", u"ug/m3LC", u"(UG/M3)", u"ug/m3 LC", u"UG/M3 LC", u"UG/M3",
                u"Micrograms/cubic meter (LC)", u"Micrograms/cubic meter (25 C)",
                u"micrograms per cubic meter (local conditions)"

            ): u"ug_m3",
            (u"cardinals",): u"cardinals"
        }

        self.error_codes = (
            u"900", 900, -999.0
        )

    def set_source(self, source):
        self.source = source

    def set_raw_name(self, raw_name):
        self.raw_name = raw_name
        self.valid_name = self.calc_valid_name()

    def set_raw_value(self, raw_value):
        self.raw_value = raw_value

    def set_raw_units(self, raw_units):
        self.raw_units = raw_units
        self.valid_units = self.calc_default_units()

    def calc_default_units(self):
        """
        Returm pollutants measurment units that have to be used.
        Based on dictionary.
        """
        flatten_default_units = chain(*self.default_unit.keys())

        if self.valid_name in flatten_default_units:
            for pollutant_group in self.default_unit:
                if self.valid_name in pollutant_group:
                    return self.default_unit.get(pollutant_group)
        elif self.valid_name not in flatten_default_units:
            self.logger.warning(u'Correct pollutant value NOT in default units list : <{0}> [{1}]'.format(
                self.valid_name,
                self.source
            ))
            return None
        else:
            self.logger.warning(u'Something goes wrong with default units: <{0}> [{1}]'.format(
                self.valid_name,
                self.source
            ))
            return None

    def validate_as_digit(self, val):
        """try to make float value from input and replace , to ."""

        if val in self.error_codes:
            self.logger.warning(u'Value looks like ERROR code, None returned : <{0}> [{1}]'.format(
                val,
                self.source
            ))

            val = None

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

    def validate_as_string(self, val):
        _val = u" ".join(val.split())


        return _val

    def calc_valid_name(self):
        """
        проганяє сирий ключ через словник значень
        :param r_key: показник забруднення
        :return: стандартизований ключ
        """
        # print(self.raw_name)
        flatten_kind = chain(*self.kind.keys())
        if self.raw_name in flatten_kind:
            for kind in self.kind:
                if self.raw_name in kind:
                    key = self.kind.get(kind)
                    return key
        elif self.raw_name not in flatten_kind:
            self.logger.warning(u'There is no such pollution name in kind list : <{0}> [{1}]'.format(
                self.raw_name,
                self.source
            ))
        else:
            self.logger.warning(u'Something goes wrong with keys: <{0}> [{1}]'.format(
                self.raw_name,
                self.source
            ))

    def calc_current_units(self):
        flatten_units = chain(*self.current_units.keys())
        if self.raw_units in flatten_units:
            for units in self.current_units:
                if self.raw_units in units:
                    key = self.current_units.get(units)
                    return key
        elif self.raw_units not in flatten_units:
            self.logger.warning(u'There is no such pollution name in current units list : <{0}> [{1}]'.format(
                self.raw_units,
                self.source
            ))
            return None
        else:
            self.logger.warning(u'Something goes wrong with keys: <{0}> [{1}]'.format(
                self.raw_units,
                self.source
            ))
            return None

    def get_name(self):
        return self.valid_name

    def get_units(self):
        return self.valid_units

    def get_value(self):
        curr = self.calc_current_units()
        curr_val = self.validate_as_string(self.raw_value) if self.is_string_unit(curr) else self.validate_as_digit(self.raw_value)
        deff = self.valid_units
        # deff = None

        # print("""IS: {0}, should be: {1}""".format(curr, deff))

        if curr is not None or deff is not None:
            if curr != deff and curr_val is not None:
                try:
                    self.valid_value = getattr(self, "_{0}_to_{1}".format(curr, deff))(curr_val)

                    return self.valid_value

                except AttributeError:
                    self.logger.warning(u'Units are not convertible (call func: _{0}_to_{1}) : |{0} to {1}| [{2}]'.format(
                        curr,
                        deff,
                        self.source
                    ))
                    return self.valid_value

            elif curr == deff:
                self.valid_value = curr_val
                return self.valid_value
        else:
            self.logger.warning(u'One of measurment units is None (call func: _{0}_to_{1}) : |{0} to {1}| [{2}]'.format(
                curr,
                deff,
                self.source
            ))
            return self.valid_value

    def is_string_unit(self, unit):
        """Check type of value in this measurement units"""
        string_units = {
            u"cardinals",
        }

        if unit in string_units:
            return True
        else:
            return False

    # converter functions
    def _degf_to_degc(self, val):
        res = (5.0/9.0) * (val-32.0)
        return res

    def _ppb_to_ppm(self, val):
        res = val / 1000
        return res

    def _ppm_to_ppb(self, val):
        res = val * 1000
        return res

    def _mph_to_ms(self, val):
        """Miles per hour to meters per second."""
        res = val * 0.44704
        return res

    def _in_to_mm(self, val):
        """Snches to millimeters."""
        res = val * 25.4
        return res

    def _wm2_to_ly(self, val):
        """Watt per square meter to langley."""
        res = val * 11.6300
        return res

    def _ml_to_km(self, val):
        """Miles to kilometers."""
        res = val * 1.609344
        return res

    def _inhg_to_kpa(self, val):
        res = val * 3.386375
        return res

    def _mbar_to_kpa(self, val):
        """Millibars to kilopascals."""
        res = val * 0.1
        return res

    def _gpa_to_kpa(self, val):
        """Gectopaskals to kilopascals."""
        res = val * 0.1
        return res

    def _mmhg_to_kpa(self, val):
        """Millibars to kilopascals."""
        res = val * 0.133322368
        return res

    def _percent_to_ppm(self, val):
        """Millibars to kilopascals."""
        res = val * 10000
        return res

    def _cardinals_to_deg(self, val):
        """Caedinal direction to kilopascals."""

        wind_dir = {
            (u"N",): u"0",
            (u"NNE",): u"22.5",
            (u"NE",): u"45",
            (u"ENE",): u"68.5",
            (u"E",): u"90",
            (u"ESE",): u"112.5",
            (u"SE",): u"135",
            (u"SSE",): u"157.5",
            (u"S",): u"180",
            (u"SSW",): u"202.5",
            (u"SW",): u"225",
            (u"WSW",): u"247.5",
            (u"W",): u"270",
            (u"WNW",): u"292.5",
            (u"NW",): u"315",
            (u"NNW",): u"337.5",
        }

        flatten_dirs = chain(*wind_dir.keys())
        if val in flatten_dirs:
            for wd in wind_dir:
                if val in wd:
                    res = wind_dir.get(wd)
                    return res
        elif val not in flatten_dirs:
            self.logger.warning(u'There is no such wind direction in cardinals: <{0}> [{1}]'.format(
                val,
                self.source
            ))
            return None
        else:
            self.logger.warning(u'Something goes wrong with keys: <{0}> [{1}]'.format(
                val,
                self.source
            ))
            return None


if __name__ == "__main__":
    p = Feature("test_spider")
    p.set_raw_name("WD")
    p.set_raw_value("NE")
    p.set_raw_units(u"cardinals")

    print(p.get_name(), p.get_value(), p.get_units())
    # print(p.calc_current_units())
    # p.convert()
    # p.valid_name = u"hum_rel"
    # p.show()
    # print(p.get_default_unit())
