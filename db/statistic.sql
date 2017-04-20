SELECT station.country, station.source, station.spider, count(*) AS count
FROM station
GROUP BY station.source
ORDER BY station.country;

ALTER TABLE station ADD COLUMN type TEXT;


SELECT country FROM station WHERE spider='california' AND type='weather';