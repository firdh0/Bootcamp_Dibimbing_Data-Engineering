CREATE TABLE IF NOT EXISTS news_articles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title TEXT,
    news_url TEXT,
    publication_at VARCHAR(100),
    content TEXT,
    scraped_at VARCHAR(100), 
    source VARCHAR(50)
);