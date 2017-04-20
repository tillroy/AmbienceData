# coding: utf-8

import pandas as pd

df1 = pd.read_csv("us_california_stations_1.txt", sep=";")
df2 = pd.read_csv("us_california_stations_2.txt", sep=";")

res = pd.merge(df1, df2, on="station_id")
res = res[[u"station_id", u"name", u"address", u"lon", u"lat", u"elev"]]

res.to_csv("us_california_stations.txt", index=False, sep=";")
