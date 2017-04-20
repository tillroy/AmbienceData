import ujson
import pandas as pd
import csv

json = open("stations.json", "r").read()
data = ujson.loads(json)


l = list()

for item in data:
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
    l.append(res)

df = pd.DataFrame(l)
# df = pd.DataFrame(l, columns=(u"station_id",))
df = df.drop_duplicates()
# print(df.groupby(by="station_id").size())


df.to_csv("stations.csv", index=False, sep="|", quotechar='"')
# df.to_csv("stations_code.csv", index=False, quoting=csv.QUOTE_NONNUMERIC, line_terminator=", u", header=False)
