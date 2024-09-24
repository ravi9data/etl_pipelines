sheet_id = "1LtI8Z37NGQsHTYH8Df1cJV6ePl5t1X4e3ZiAwdaxwKg"
bucket_name = "grover-eu-central-1-production-data-science"
campaign_data_path = "/price_elasticity/output_dir/date_re/campaign_data/"
campaign_sku_wo_campaign_id_path = \
    "/price_elasticity/output_dir/date_re/sku_price_data/campaign_sku_wo_campaign_id.csv"
price_query_path = "price_elasticity/input_dir/price_data.sql"
price_data_path = "/price_elasticity/output_dir/date_re/sku_price_data/price_data.csv"
fuel_prices_url = "https://ec.europa.eu/energy/observatory/reports/Oil_Bulletin_Prices_History.xlsx"

holidays_de_2022_path = "price_elasticity/input_dir/holidays_de_2022.json"
holidays_de_2023_path = "price_elasticity/input_dir/holidays_de_2023.json"
final_models_path = "/price_elasticity/output_dir/date_re/final_models/daily_data_EU_4.csv"
dfs_query = """SELECT m.*, c.is_campaign from df_main m left join df_campaign c on \
(m.product_sku =c.sku and m.snapshot_date = c.campaign_date)"""
glue_models_path = "/price_elasticity/output_dir/glue_final_models/"
price_elasticity_table = "price_elasticity_model_v1"
price_elasticity_schema = "data_production_price_elasticity"
