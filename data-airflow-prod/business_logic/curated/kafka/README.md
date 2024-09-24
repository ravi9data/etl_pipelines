# Curated Kafka Topics

## Internal Billing Payments

This pipeline makes the following transformations:
1. Flattens the payload. Getting the [fields](https://github.com/devsbb/data-airflow/blob/main/business_logic/curated/kafka/internal_billing_payments.py#L48) into columns
2. Explodes Column *'line_items'*
3. Adds a column named *'line_item_positon'* which represents the position of the line item in the field array *'line_items'*
4. Replace the content of the column *orders* with a json with 2 fields *order_tax_rate* and *order_tax_rate* Or None when there is no data available.
