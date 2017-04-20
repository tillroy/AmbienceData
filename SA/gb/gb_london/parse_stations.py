from lxml import etree
import pandas as pd

xml = etree.parse("stations.xml")
markers = xml.xpath("/markers/marker")



l = list()
for marker in markers:
    lon = marker.xpath("@lng")[0]
    lat = marker.xpath("@lat")[0]
    name = marker.xpath("@label")[0]
    code = marker.xpath("@code")[0]

    res = {
        # "lon": lon,
        # "lat": lat,
        # "station_name": name,
        "station_id": '|{0}|'.format(code),
        "address": "-",
    }
    l.append(res)

df = pd.DataFrame(l, columns=("station_id", "address"))

df.to_csv("stations_code.csv", index=False, sep=",", quotechar='"')
