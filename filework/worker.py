# coding: utf-8
import pandas
from numpy import isnan

country = "us"
spider = "cincinnati"
_type = "pollution"
source_name = "http://www.southwestohioair.org"

in_file_name = "{0}_{1}_{2}_stations.txt".format(country, spider, _type)

out_file_name = "add_station_{0}_{1}_{2}.sql".format(country, spider, _type)


data_frame = pandas.read_csv(in_file_name, ";", dtype="str")
data_frame['elev'] = data_frame['elev'].astype(float)

# print(data_frame)
open(out_file_name, "w").close()
# print(data_frame)

for iitem in range(data_frame.shape[0]):
    row = data_frame.iloc[iitem, ]
    sqlfile = open(out_file_name, "a")

    address = str(row.address)
    if "'" in address:
        address = address.replace("'", "''")

    station_name = str(row.station_name)
    if "'" in station_name:
        station_name = row.station_name.replace("'", "''")

    if isnan(row.elev):
        elev = 'NULL'
    else:
        elev = "'{0}'".format(row.elev)

    record = """
    INSERT INTO station(lon, lat, address, code, source, name, country, spider, elev, type)
    VALUES ({0}, {1}, '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', {8}, '{9}');
    """.format(row.lon, row.lat, address, row.station_id, source_name, station_name, country, spider, elev, _type)

    # record_one = """
    # INSERT INTO scrapper_station(lon, lat, address, source_id, source, st_name)
    # VALUES ({0}, {1}, '{2}', '{3}', '{4}', '{5}');
    # """.format(row.lon, row.lat, row.address, row.station_id, source_name, row.station_name)
    #
    # record_two = """
    # INSERT INTO scrapper_map(lon, lat, address, source_id, source, st_name)
    # VALUES ({0}, {1}, '{2}', '{3}', '{4}', '{5}');
    # """.format(row.lon, row.lat, row.address, row.station_id, source_name, row.station_name)

    sqlfile.write(record)
