CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    title TEXT,
    news_url TEXT UNIQUE,
    publication_at VARCHAR(100),
    content TEXT,
    scraped_at TIMESTAMP WITHOUT TIME ZONE,
    source VARCHAR(50)
);