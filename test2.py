
from w3lib import url

from urlparse import urlparse


a = "redir.pdasdasdasdahp?loc_url=http%3A%2F%2Fwww.arb.ca.gov%2Faqmis2%2Fdisplay.php%3Freport%3DSITE31D%26site%3D3017%26year%3D2017%26mon%3D05%26day%3D04%26hours%3Dall%26statistic%3DHVAL%26ptype%3Daqd%26param%3DOZONE_ppm"

def valideate_url(url):
    try:
        res = url.split("loc_url=")[1]
        res = res.replace("%2F", "/")
        res = res.replace("%3A", ":")
        res = res.replace("%3F", "?")
        res = res.replace("%3D", "=")
        res = res.replace("%26", "&")
        return res
    except IndexError:
        return None

print(valideate_url(a))
