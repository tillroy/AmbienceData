# coding: utf-8
import sqlite3
import pandas as pd
import os


def get_station_data(code, name):
    conn = sqlite3.connect('station.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT code, name, address, lon, lat
        FROM station WHERE code="{0}" AND name="{1}";""".format(code, name))

    res = cursor.fetchone()
    conn.close()
    return res


def show_source():
    conn = sqlite3.connect('station.db')
    cursor = conn.cursor()

    cursor.execute(u"""
        SELECT source, count(name), country, spider, type
        FROM station GROUP BY source, country, spider
        ORDER BY country ASC, count(name) DESC;
        """)

    sources = cursor.fetchall()
    conn.close()
    if sources:
        header = u"|{:<4.4}|{:^5.5}|{:^45.45}|{:^20.20}|{:^10.10}|".format(u"Code", u"Count", u"URL", u"Spider", u"Type")
        print(header)
        line = u"|{:-^4.4}|{:-^5.5}|{:-^45.45}|{:-^20.20}|{:-^10.10}|".format(u"-", u"-", u"-", u"-", u"-")
        print(line)
        for source in sources:
            url = str(source[0])
            count = str(source[1])
            country = str(source[2]).upper()
            spider = str(source[3])
            _type = str(source[4])

            res_str = u"|{:<4.4}|{:^5.5}|{:^45.45}|{:<20.20}|{:<10.10}|".format(country, count, url, spider, _type)
            print(res_str)

        print(u"{0} sources.".format(len(sources)))


def update_map_info():
    conn = sqlite3.connect('station.db')
    cursor = conn.cursor()

    cursor.execute(u"""
            SELECT source, country, spider, type
            FROM station GROUP BY source, country, spider;
            """)

    sources = cursor.fetchall()
    conn.close()

    open(u"update_map_info.sql", "w").close()
    for source in sources:
        url = str(source[0])
        country = "'" + str(source[1]) + "'" if source[1] else "NULL"
        spider = "'" + str(source[2]) + "'" if source[2] else "NULL"
        _type = "'" + str(source[3]) + "'" if source[3] else "NULL"

        sql_str = u"""
            UPDATE scrapper_map
            SET country = {country}, spider_name = {spider_name}, spider_type = {spider_type}
            WHERE source = '{source}';
            """.format(
            country=country,
            spider_name=spider,
            spider_type=_type,
            source=url
        )
        open(u"update_map_info.sql", "a").write(sql_str.encode("utf-8"))



def make_csv(country):
    conn = sqlite3.connect('station.db')
    cursor = conn.cursor()

    cursor.execute(u"""
        SELECT code, name, lon, lat, spider
        FROM station
        WHERE country LIKE "{0}"
        """.format(country))

    sources = cursor.fetchall()
    conn.close()

    data = list()
    for record in sources:
        code = record[0]
        name = record[1]
        lon = record[2]
        lat = record[3]
        spider = record[4]

        res = {
            u"code": code,
            u"name": name,
            u"lon": lon,
            u"lat": lat,
            u"spider": spider,
        }
        data.append(res)

    df = pd.DataFrame(data)
    df.to_csv("all_stations.csv", sep="|", index=False, encoding="utf-8")

    # csv = open(u"all_station.csv", "w")
    # csv.write(", ".join(("code", "name", "lon", "lat", "\n")))
    # csv.close()
    #
    # for source in sources:
    #     source = [unicode(x) for x in source]
    #     res = ", ".join(source)
    #     open("all_station.csv", "a").write(res + u"\n")


def make_sql(spider_name, spider_type, *args):
    conn = sqlite3.connect('station.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT country
        FROM station WHERE spider='{0}' AND type='{1}';""".format(spider_name, spider_type))

    # print(spider_name, spider_type, cursor.fetchone())

    country = cursor.fetchone()[0]

    cursor.execute("""
        SELECT lon, lat, address, code, name, source
        FROM station WHERE spider='{0}' AND type='{1}';""".format(spider_name, spider_type))

    res = cursor.fetchall()
    conn.close()

    sql_dir = "postgresql_sql"


    open(sql_dir + u"/{0}/add_station_{0}_{1}_{2}.sql".format(country, spider_name, spider_type), "w").close()
    for record in res:
        for db_name in args:
            lon = str(record[0])
            lat = str(record[1])
            address = record[2]
            address = address.replace(u"'", u"''") if u"'" in address else address

            code = record[3]
            name = record[4]
            name = name.replace(u"'", u"''") if u"'" in name else name
            source = record[5]

            sql_str = u"""
                INSERT INTO {db_name}(lon, lat, address, source_id, st_name, source)
                VALUES({lon},{lat},'{address}','{code}','{name}', '{source}');
            """.format(
                db_name=db_name,
                lon=lon,
                lat=lat,
                address=address,
                code=code,
                name=name,
                source=source
            )
            open(sql_dir + u"/{0}/add_station_{0}_{1}_{2}.sql".format(country, spider_name, spider_type), "a").write(sql_str.encode("utf-8"))


def delete(field, value):
    conn = sqlite3.connect('station.db')
    cursor = conn.cursor()

    cursor.execute("""
        DELETE FROM station
        WHERE {0} = '{1}';
        """. format(field, value))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    pass

    # t = get_station_data(code, name)

    show_source()
    # delete("spider", "gpmn_parks")
    # make_csv("*")

    # make_sql(u"texas", u"pollution", u"scrapper_station", u"scrapper_map")

    # make_sql(u"california", u"weather", u"scraper_current_weather_data")
    # make_csv("us")
    update_map_info()


    # england scotland wales northern_ireland
