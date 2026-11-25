CREATE KEYSPACE IF NOT EXISTS project3
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS project3.expenses (
    user_id int,
    date_time timestamp,
    description text,
    type text,
    count int,
    payment decimal,
    PRIMARY KEY (user_id, date_time)
) WITH CLUSTERING ORDER BY (date_time ASC);
