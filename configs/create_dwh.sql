CREATE DATABASE IF NOT EXISTS social_dwh;
CREATE TABLE social_dwh.dim_source
(
    source_id       UInt32,
    source_name     String,
    source_type     String,
    load_dt         DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY source_id;

CREATE TABLE social_dwh.dim_date
(
    date_id       UInt32,          
    date_value    Date,
    year          UInt16,
    month         UInt8,
    day           UInt8,
    load_dt       DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY date_id;

CREATE TABLE social_dwh.dim_topic
(
    topic_id     UInt32,
    topic_name   String,
    load_dt      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY topic_id;

CREATE TABLE social_dwh.fact_sentiment
(
    fact_id          UUID,
    source_id        UInt32,
    date_id          UInt32,
    topic_id         UInt32,
    text             String,
    sentiment_class  String,
    sentiment_score  Float32,
    word_count       UInt32,
    hashtags_count   UInt32,
    created_at       DateTime
)
ENGINE = MergeTree()
ORDER BY (date_id, source_id, fact_id);
