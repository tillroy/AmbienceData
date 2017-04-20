SELECT source, count(name) AS count, country, spider
FROM station GROUP BY source ORDER BY country, spider;