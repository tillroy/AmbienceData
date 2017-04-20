CREATE TABLE station (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    country TEXT NOT NULL,
    spider TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    address TEXT NOT NULL,
    lon FLOAT NOT NULL,
    lat FLOAT NOT NULL,
    elev FLOAT
);