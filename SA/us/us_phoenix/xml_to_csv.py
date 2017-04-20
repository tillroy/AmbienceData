from scrapy import Selector


data = open("sites.xml", "r").read()

res = Selector(text=data)
record = res.xpath("*//gage_wx")
open("sites.csv", "w").close()
open("sites.csv", "a").write("lon|lat|elev|name\n")
for el in record:
    lon = el.xpath('@long').extract_first()
    lat = el.xpath('@lat').extract_first()
    elev = el.xpath('@elev').extract_first()
    name = el.xpath('@name').extract_first()
    res = "|".join((lon, lat, elev, name, "\n"))

    open("sites.csv", "a").write(res)

