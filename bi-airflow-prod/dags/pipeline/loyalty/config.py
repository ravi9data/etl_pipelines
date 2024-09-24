loyalty_pipeline_config = {
   "loyalty": {
      "directory": "loyalty",
      "script_name": [
         "00_guest_signup",
         "01_guest_submitted_order",
         "02_guest_approved_order",
         "03_guest_revoked",
         "04_guest_order_paid",
         "05_guest_contract_started",
         "06_host_referred",
         "06_master_host_guest_mapping",
         "07_master_loyalty_customer",
         "08_master_loyalty_subscription",
         "09_master_Loyalty_order"
      ]
   }
}
