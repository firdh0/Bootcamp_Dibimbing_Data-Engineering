CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    hour SMALLINT,
    minute SMALLINT
);

CREATE TABLE dim_day (
    day_id SERIAL PRIMARY KEY,
    name VARCHAR(20) NOT NULL
);

CREATE TABLE dim_month (
    month_id SERIAL PRIMARY KEY,
    name VARCHAR(20) NOT NULL
);

CREATE TABLE dim_quartal (
    quartal_id SERIAL PRIMARY KEY,
    name VARCHAR(20) NOT NULL
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    day_id SMALLINT NOT NULL,
    month_id SMALLINT NOT NULL,
    quartal_id SMALLINT NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    FOREIGN KEY (day_id) REFERENCES dim_day(day_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (month_id) REFERENCES dim_month(month_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (quartal_id) REFERENCES dim_quartal(quartal_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL,
    time_id INT NOT NULL,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE dim_article (
    article_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    url VARCHAR(255) NOT NULL,
    content TEXT NOT NULL
);

CREATE TABLE dim_media (
    media_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE fact_analytics (
    analytic_id SERIAL PRIMARY KEY,
    article_id INT NOT NULL,
    category_id INT NOT NULL,
    media_id INT NOT NULL,
    publish_datetime_id INT NOT NULL,
    scrape_datetime_id INT NOT NULL,
    word_content_count INT NOT NULL,
    word_title_count INT NOT NULL,
    estimated_reading_time_minutes INT NOT NULL,
    readibility_score REAL NOT NULL,
    FOREIGN KEY (article_id) REFERENCES dim_article(article_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (media_id) REFERENCES dim_media(media_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (publish_datetime_id) REFERENCES dim_datetime(datetime_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (scrape_datetime_id) REFERENCES dim_datetime(datetime_id) ON UPDATE CASCADE ON DELETE RESTRICT
);
