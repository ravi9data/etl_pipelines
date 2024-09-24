monthly_load_pipeline_config = {
   "dwh_finance_monthly": {
      "directory": "4_dwh_finance/monthly_reports",
      "script_name": [
         "earned_revenue_historical",
         "depreciation_report",
         "asset_collection_curves_aggregated"
      ]
   },
   "dwh_dc_monthly": {
      "directory": "4_dwh_dc",
      "script_name": [
         "subscription_performance"
      ]
   }
}
