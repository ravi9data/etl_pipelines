# Query's to get the eans and upcs as inouts
EANS_QUERY = '''SELECT DISTINCT(full_ean) FROM public.mozenda_input_eu;'''

UPCS_QUERY = '''SELECT DISTINCT(full_upcs) FROM public.mozenda_input_us;'''

# Query's to clear the eans and upcs of current day
EANS_QUERY_TO_CLEAR = '''DELETE FROM pricing.ean_asin WHERE "date" = CURRENT_DATE;'''

UPCS_QUERY_TO_CLEAR = '''DELETE FROM pricing.upcs_asin WHERE "date" = CURRENT_DATE;'''
