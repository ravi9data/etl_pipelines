-- master.addon_35up_order definition

-- Drop table

-- DROP TABLE master.addon_35up_order;

--DROP TABLE master.addon_35up_order;
CREATE TABLE IF NOT EXISTS master.addon_35up_order (
    order_id VARCHAR(65535) NOT NULL ENCODE LZO PRIMARY KEY,
    customer_id INTEGER ENCODE AZ64,
    created_date TIMESTAMP WITH TIME ZONE ENCODE AZ64,
    submitted_date TIMESTAMP WITH TIME ZONE ENCODE AZ64,
    paid_date TIMESTAMP WITH TIME ZONE ENCODE AZ64,
    status VARCHAR(765) ENCODE LZO,
    order_value NUMERIC(10,2) ENCODE AZ64,
    new_recurring VARCHAR(9) ENCODE LZO,
    store_country VARCHAR(510) ENCODE LZO,
    customer_type VARCHAR(65535) ENCODE LZO,
    order_item_count BIGINT ENCODE AZ64,
    store_code VARCHAR(256) ENCODE LZO,
    refund_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    addon_item_count BIGINT ENCODE AZ64,
    addon_price NUMERIC(38,4) ENCODE AZ64
)
DISTSTYLE AUTO;
