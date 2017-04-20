# coding: utf-8
import csv

import ujson
import pandas as pd

json = open("test.json", "r").read()
data = ujson.loads(json)


l = list()

for group in data:
    for item in group:
        lon = item["longitude"]
        lat = item["latitude"]
        name = item["site_name"]
        code = item["site_id"]

        res = {
            "lon": lon,
            "lat": lat,
            "station_name": name,
            "station_id": code,
            "address": name,
        }
        if "closed" not in item["summary"]:
            l.append(res)

df = pd.DataFrame(l)
# df = pd.DataFrame(l, columns=(u"station_id",))
df = df.drop_duplicates()
# print(df.groupby(by="station_id").size())


df.to_csv("test_stations.csv", index=False, sep="|", quotechar='"')
# df.to_csv("stations_code.csv", index=False, quoting=csv.QUOTE_NONNUMERIC, line_terminator=", u", header=False)
