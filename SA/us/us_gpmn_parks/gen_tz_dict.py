# -*- coding: utf-8 -*-

import pandas as pd


df = pd.read_csv("./station_timezone.txt", sep=";")

grouped = df.groupby(by="timezone")

res = dict()
for gr_name, gr_value in grouped:
    keys = tuple([el[0] for el in gr_value.itertuples(index=False)])
    res[keys] = gr_name

open("tz_dict.txt", "w").write(str(res))
    # print("a")
# print(df["station_id"])
