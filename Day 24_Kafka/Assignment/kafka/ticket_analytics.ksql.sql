-- Langkah 1: Buat STREAM dari topik Kafka yang berisi data Protobuf
CREATE STREAM ticket_purchase_stream (
    purchase_id VARCHAR,
    user_id VARCHAR,
    user_name VARCHAR,
    ticket_tier VARCHAR,
    purchase_type VARCHAR,
    price BIGINT,
    quantity INT,
    purchase_timestamp BIGINT
) WITH (
    KAFKA_TOPIC = 'concert_ticket_purchases',
    VALUE_FORMAT = 'PROTOBUF',
    VALUE_PROTOBUF_SCHEMA_ID = 1 -- Ganti dengan ID Skema Anda jika diperlukan
);

-- Langkah 2: Buat TABLE untuk menghitung total pendapatan dan tiket terjual per tier
-- Tabel ini akan terus diperbarui secara real-time
CREATE TABLE revenue_by_tier WITH (
    KAFKA_TOPIC = 'revenue_by_tier_topic',
    VALUE_FORMAT = 'JSON'
) AS
SELECT
    ticket_tier,
    COUNT(*) AS tickets_sold,
    SUM(price * quantity) AS total_revenue
FROM
    ticket_purchase_stream
GROUP BY
    ticket_tier
EMIT CHANGES;

-- Langkah 3 (Opsional): Jalankan kueri untuk melihat hasilnya secara langsung
-- SELECT * FROM revenue_by_tier EMIT CHANGES;