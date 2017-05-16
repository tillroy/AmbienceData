import sqlite3


class StationsData(object):
    """SQLite database for station data, their location etc."""

    def __init__(self):
        self.conn = sqlite3.connect('/mnt/hdd/pycharm_projects/AmbienceData/db/station.db')
        self.cursor = self.conn.cursor()

    def get_station(self, code, source):
        # lon, lat, address, source_id, st_name, source
        self.cursor.execute("""
            SELECT source, code, name, address, country, spider, type, lon, lat
            FROM station WHERE code="{0}" AND source="{1}";
            """.format(code, source))

        res = self.cursor.fetchone()
        col_names = ("source", "code", "name", "address", "country", "spider", "type", "lon", "lat")
        res = dict(zip(col_names, res)) if res is not None else None
        # self.conn.close()
        return res

    def close(self):
        self.conn.close()

s = StationsData()

print(s.get_station("20", "http://dec.alaska.gov"))
# print(s.get_station("2", "http://dec.alaska.gov"))
# print(s.get_station("5", "http://dec.alaska.gov"))
print(s.get_station("ddd", "http://dec.alaska.gov"))
