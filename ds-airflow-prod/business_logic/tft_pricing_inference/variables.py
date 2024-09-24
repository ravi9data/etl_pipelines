bucket_name = "grover-eu-central-1-production-data-science"
final_models_path = "/price_elasticity/output_dir/date_re/final_models/daily_data_EU_4.csv"
inference_models_path = "/price_elasticity/price_elasticity_forecast/date_re"
data_limit_to_20_runs = """DELETE FROM pricing.price_elasticity_forecast WHERE \
snapshot_date < GETDATE() - INTERVAL '21 weeks'"""
pricing_table = "price_elasticity_forecast"
pricing_schema = "pricing"
