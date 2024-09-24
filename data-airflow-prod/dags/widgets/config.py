widget_config = [
    {
        'table_name': 'widget_most_popular',
        'schema': 'product_requests',
        'bucket_var': 'widget_bucket',
        'prefix': 'top_ordered_products/',
        'grouped_by': ['store_code'],
        'agg_element': 'product_sku',
        'column_name': 'productSkus',
        'sort_column': ['rank_product']
    },
    {
        'table_name': 'random_cat_top_products',
        'schema': 'product_requests',
        'bucket_var': 'widget_bucket',
        'prefix': 'random_category_top_products/',
        'grouped_by': ['store_code', 'subcategory_id'],
        'agg_element': 'product_sku',
        'column_name': 'productSkus',
        'sort_column': ['product_rank']
    },
    {
        'table_name': 'widget_feed',
        'schema': 'product_requests',
        'bucket_var': 'widget_bucket',
        'prefix': 'top_products/',
        'grouped_by': ['store_code'],
        'agg_element': 'product_sku',
        'column_name': 'productSkus',
        'sort_column': ['rank_product']
    },
    {
        'table_name': 'widget_our_best_deals',
        'schema': 'product_requests',
        'bucket_var': 'widget_bucket',
        'prefix': 'deal_products/',
        'grouped_by': ['store_code'],
        'agg_element': 'product_sku',
        'column_name': 'productSkus',
        'sort_column': ['rank_product']
    },
    {
        'table_name': 'widget_new_arrivals',
        'schema': 'product_requests',
        'bucket_var': 'widget_bucket',
        'prefix': 'new_products/',
        'grouped_by': ['store_code'],
        'agg_element': 'product_sku',
        'column_name': 'productSkus',
        'sort_column': ['rank_product']
    }
]
