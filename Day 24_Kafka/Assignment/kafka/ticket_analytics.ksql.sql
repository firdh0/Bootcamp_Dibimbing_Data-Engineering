CREATE STREAM ticket_purchase_stream (
    purchase_id VARCHAR, 
    user_id VARCHAR, 
    user_name VARCHAR, 
    ticket_tier VARCHAR, 
    purchase_type VARCHAR, 
    price BIGINT, 
    quantity INT, 
    purchase_timestamp BIGINT
) WITH (KAFKA_TOPIC = 'concert_ticket_purchases', VALUE_FORMAT = 'PROTOBUF');

CREATE TABLE revenue_by_tier AS SELECT ticket_tier, COUNT(*) AS tickets_sold, SUM(price * quantity) AS total_revenue FROM ticket_purchase_stream GROUP BY ticket_tier EMIT CHANGES;

SELECT * FROM revenue_by_tier EMIT CHANGES;