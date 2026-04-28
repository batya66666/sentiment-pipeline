CREATE KEYSPACE IF NOT EXISTS social_staging
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE social_staging;

CREATE TABLE IF NOT EXISTS raw_data (
    id text PRIMARY KEY, 
    source text,
    text text,
    created_at text
);