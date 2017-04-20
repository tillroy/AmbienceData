# coding: utf-8

import ujson
json_file = open("row_data.json", "r").read()

json = ujson.loads(json_file)
open("inea.csv", "w").close()
for el in json:
    station_name = el[0]
    lon = el[1]
    lat = el[2]

    addr = el[8]
    if addr == "":
        addr = "NA"
    pm10 = el[10]
    so2 = el[11]
    no2 = el[12]
    o3 = el[13]
    co = el[14]
    pts = el[15]

    print(pm10)


    res = ",".join((station_name, str(lon), str(lat), "\n"))
    open("inea.csv", "a").write(res)