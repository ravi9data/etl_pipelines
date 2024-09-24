sf_obj_sync_config = [
    {"name": "Product2", "load_type": "incremental"},
    {"name": "asset_payment__c", "load_type": "incremental"},
    {"name": "customer_asset_allocation__c", "load_type": "incremental"},
    {"name": "purchase_request__c", "load_type": "full_load", "executor_config": {
        "KubernetesExecutor": {
            "request_memory": "500Mi",
            "request_cpu": "500m",
            "limit_memory": "5G",
            "limit_cpu": "1500m"
        }
    }},
    {"name": "asset", "load_type": "incremental", "executor_config": {
        "KubernetesExecutor": {
            "request_memory": "1G",
            "request_cpu": "2000m",
            "limit_memory": "8G",
            "limit_cpu": "2000m"
        }
    }},
    {"name": "Account", "load_type": "incremental"},
    {"name": "Supplier__c", "load_type": "incremental"},
    {"name": "User", "load_type": "incremental"},
    {"name": "capital_source__c", "load_type": "incremental"},
    {"name": "store__c", "load_type": "incremental"},
    {"name": "Task", "load_type": "incremental"},
    {"name": "Contact", "load_type": "incremental"},
    {"name": "refund_payment__c", "load_type": "incremental"},
    {"name": "purchase_request_item__c", "load_type": "full_load", "executor_config": {
        "KubernetesExecutor": {
            "request_memory": "500Mi",
            "request_cpu": "500m",
            "limit_memory": "5G",
            "limit_cpu": "1500m"
        }
    }},
    {"name": "asset_insurance__c", "load_type": "incremental"},
    {"name": "asset_request__c", "load_type": "incremental"},
    {"name": "orderitem", "load_type": "incremental"},
    {"name": "order", "load_type": "incremental"},
    {"name": "subscription__c", "load_type": "incremental"},
    {"name": "subscription_payment__c", "load_type": "incremental", "executor_config": {
        "KubernetesExecutor": {
            "request_memory": "500Mi",
            "request_cpu": "500m",
            "limit_memory": "5G",
            "limit_cpu": "1500m"
        }
    }}]

sf_hist_obj_sync_config = [
    {"name": "AssetHistory", "load_type": "incremental"},
    {"name": "OrderHistory", "load_type": "incremental"},
    {"name": "asset_payment__History", "load_type": "incremental"},
    {"name": "customer_asset_allocation__History", "load_type": "incremental"},
    {"name": "subscription__History", "load_type": "incremental"},
    {"name": "subscription_payment__History", "load_type": "incremental"}]
