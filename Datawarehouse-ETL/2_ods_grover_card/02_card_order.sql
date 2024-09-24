----------------Master Card_order
with card as(
select 
    distinct customer_id,
    first_value(first_event_timestamp) over (partition by customer_id order by first_event_timestamp asc
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as First_card_created_date
SELECT 
    o.order_id
    ,COALESCE(c.customer_id,cc.customer_id) AS customer_id
    ,cc.customer_type
    ,o.created_date 
    ,o.submitted_date
    ,approved_date
    ,o.canceled_date 
    ,o.status
    ,o.order_value 
    ,o.voucher_code 
    ,o.voucher_type
    ,o.voucher_value 
    ,o.voucher_discount 
  	 ,r.new_recurring 
    ,st.store_name
    ,basket_size
    ,order_item_count
    ,1 AS cart_orders 
    ,ocl.cart_page_orders 
    ,ocl.completed_orders AS submitted_orders
    ,ocl.paid_orders 
    ,CASE WHEN o.created_date>c.First_card_created_date THEN 1 ELSE 0 END is_created_after_card
    ,CASE WHEN o.submitted_Date>c.First_card_created_date THEN 1 ELSE 0 END is_submitted_after_card
FROM ods_production."order" o 
LEFT JOIN card c
    ON c.customer_id=o.customer_id
LEFT JOIN ods_production.customer cc
	 ON cc.customer_id =o.customer_id 
LEFT JOIN ods_production.order_retention_group r 
    ON r.order_id=o.order_id
LEFT JOIN ods_production.store st
    ON st.id=o.store_id
LEFT JOIN ods_production.order_conversion_labels ocl 
    ON ocl.order_id=o.order_id