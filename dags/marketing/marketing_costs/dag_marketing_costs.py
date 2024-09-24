import datetime
import logging

import sqlalchemy
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

from dags.marketing.marketing_costs.run_sql import \
    run_marketing_costs_daily_importer
from plugins.dag_utils import get_schedule, on_failure_callback
from plugins.operators.run_query_from_repo_operator import \
    RunQueryFromRepoOperator
from plugins.utils.gsheet import load_data_from_gsheet

DAG_ID = 'marketing_costs'
GOOGLE_CLOUD_CONN = 'google_cloud_default'

AFFILIATE_COSTS_SHEET_ID = '1VoeK_VpwHGBL2kuUBrMcR_ro6ByxP98SnzkzL0W1_lM'
CHANNEL_MAPPING_SHEET_ID = '1zEk_T-l5oaEcUkkl2eq9t-JV1kFeDMSAjKFVglbLmFs'
MANUAL_COST_SHEET_ID = '1p3Ix_Bmdcj23Isoh8br8--D0x6pcwZ_6vmPSRhjagUY'
EBAY_KLEINANZEIGEN_SHEET_ID = '1KKKRdgnkAL-kSRtVxzLZvyE8cbfU667qsmzP_0NlF5s'
EBAY_KLEINANZEIGEN_CAMPAIGN_SHEET_ID = '1Z_gwbTabtxGjcAYFtKXUgDTS87FDznbctb2p3TbtVbY'
YAKK_B2B_COST_SHEET_ID = '1in0x6G20Zw1XoLcMwFGPRVoApOOYfRdnfHn-vfV3LDY'
SUPERMETRICS_SHEET_ID = '1MwQf44k3ZNNd9JCQyU7gX7JmzYKVqNadg_UVzIWcio4'
MARKTPLAATS_COST_SHEET_ID = '1xyghASzOdY15vHfZU1we_KmRixR5uIxku50jJIqGpq4'
MILANUNCIOS_COST_SHEET_ID = '1ckK2pd0p-GUNFVZZb4MjuwLJAM8Mk094I5gfyjlPLxg'

EXECUTOR_CONFIG = {
    'KubernetesExecutor': {
        'request_memory': '100Mi',
        'request_cpu': '100m',
        'limit_memory': '4G',
        'limit_cpu': '2000m'
    }
}

logger = logging.getLogger(__name__)

default_args = {'owner': 'bi-eng',
                'depends_on_past': False,
                'start_date': datetime.datetime(2022, 10, 20),
                'retries': 2,
                'retry_delay': datetime.timedelta(seconds=30),
                'on_failure_callback': on_failure_callback}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=get_schedule('15 6,8 * * *'),
          max_active_runs=1,
          catchup=False,
          tags=[DAG_ID, 'marketing', 'marketing_cost'])

marketing_affiliate_fixed_costs = PythonOperator(
    dag=dag,
    task_id="load_affiliate_fixed_costs",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": AFFILIATE_COSTS_SHEET_ID,
               "range_name": 'Fixed_Costs',
               "target_table": "marketing_affiliate_fixed_costs",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "affiliate": sqlalchemy.types.VARCHAR(length=65535),
                              "affiliate_network": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535)
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

# marketing_affiliate_order_costs = PythonOperator(
#     dag=dag,
#     task_id="load_affiliate_order_costs",
#     python_callable=load_data_from_gsheet,
#     op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
#                "sheet_id": AFFILIATE_COSTS_SHEET_ID,
#                "range_name": 'Order_Costs',
#                "target_table": "marketing_affiliate_order_costs",
#                "target_schema": "staging",
#                "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
#                               "country": sqlalchemy.types.VARCHAR(length=65535),
#                               "currency": sqlalchemy.types.VARCHAR(length=65535),
#                               "order_id": sqlalchemy.types.VARCHAR(length=65535),
#                               "affiliate": sqlalchemy.types.VARCHAR(length=65535),
#                               "affiliate_network": sqlalchemy.types.VARCHAR(length=65535),
#                               "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535)
#                               }
#                },
#     executor_config=EXECUTOR_CONFIG
# )

marketing_channel_mapping = PythonOperator(
    dag=dag,
    task_id="load_channel_mapping",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": CHANNEL_MAPPING_SHEET_ID,
               "range_name": 'Sheet1',
               "target_table": "marketing_cost_channel_mapping_sheet1",
               "target_schema": "staging",
               "data_types": {"account": sqlalchemy.types.VARCHAR(length=65535),
                              "channel": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "cash_non_cash": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_type": sqlalchemy.types.VARCHAR(length=65535),
                              "brand_non_brand": sqlalchemy.types.VARCHAR(length=65535),
                              "channel_detailed": sqlalchemy.types.VARCHAR(length=65535),
                              "channel_grouping": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name_contains": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name_contains2": sqlalchemy.types.VARCHAR(length=65535),
                              "reporting_grouping_name": sqlalchemy.types.VARCHAR(length=65535),
                              "advertising_channel_type": sqlalchemy.types.VARCHAR(length=65535),
                              "medium": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_group_name_contains": sqlalchemy.types.VARCHAR(
                                  length=65535),
                              "campaign_name_does_not_contain": sqlalchemy.types.VARCHAR(
                                  length=65535)
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_billboard_cost = PythonOperator(
    dag=dag,
    task_id="marketing_billboard_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'billboard_cost',
               "target_table": "marketing_billboard_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "brand_non_brand": sqlalchemy.types.VARCHAR(length=65535),
                              "cash_percentage": sqlalchemy.types.VARCHAR(length=65535),
                              "gross_actual_spend": sqlalchemy.types.VARCHAR(length=65535),
                              "gross_budget_spend": sqlalchemy.types.VARCHAR(length=65535),
                              "non_cash_percentage": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_criteo_cost = PythonOperator(
    dag=dag,
    task_id="marketing_criteo_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": 'criteo_supermetrics',
               "target_table": "marketing_criteo_supermetric",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "account_name": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                              "ad_set_name": sqlalchemy.types.VARCHAR(length=65535),
                              "cost": sqlalchemy.types.VARCHAR(length=65535),
                              "clicks": sqlalchemy.types.VARCHAR(length=65535),
                              "impressions": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_influencer_cost_weekly = PythonOperator(
    dag=dag,
    task_id="marketing_influencer_cost_weekly",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'influencer_cost_weekly',
               "target_table": "marketing_influencer_cost_weekly",
               "target_schema": "staging",
               "data_types": {"country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "week_date": sqlalchemy.types.VARCHAR(length=65535),
                              "customer_type": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_partnership_cost = PythonOperator(
    dag=dag,
    task_id="marketing_partnership_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'partnership_cost',
               "target_table": "marketing_partnership_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=1000),
                              "country": sqlalchemy.types.VARCHAR(length=1000),
                              "currency": sqlalchemy.types.VARCHAR(length=1000),
                              "cost_type": sqlalchemy.types.VARCHAR(length=1000),
                              "partner_name": sqlalchemy.types.VARCHAR(length=1000),
                              "is_test_and_learn": sqlalchemy.types.VARCHAR(length=1000),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=1000),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=1000),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_podcasts_cost = PythonOperator(
    dag=dag,
    task_id="marketing_podcasts_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'podcasts_cost',
               "target_table": "marketing_podcasts_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "podcast_name": sqlalchemy.types.VARCHAR(length=65535),
                              "is_test_and_learn": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_reddit_cost = PythonOperator(
    dag=dag,
    task_id="marketing_reddit_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'reddit_cost',
               "target_table": "marketing_reddit_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                              "costs": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_sponsorships_cost = PythonOperator(
    dag=dag,
    task_id="marketing_sponsorships_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'sponsorships_cost',
               "target_table": "marketing_sponsorships_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "cost": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "voucher": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_retail_cost = PythonOperator(
    dag=dag,
    task_id="marketing_retail_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'retail_cost',
               "target_table": "marketing_retail_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "channel": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "cost_type": sqlalchemy.types.VARCHAR(length=65535),
                              "placement": sqlalchemy.types.VARCHAR(length=65535),
                              "retail_name": sqlalchemy.types.VARCHAR(length=65535),
                              "is_test_and_learn": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_seo_cost = PythonOperator(
    dag=dag,
    task_id="marketing_seo_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'seo_cost',
               "target_table": "marketing_seo_cost",
               "target_schema": "staging",
               "data_types": {"url": sqlalchemy.types.VARCHAR(length=65535),
                              "date": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "is_test_and_learn": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_tv_cost = PythonOperator(
    dag=dag,
    task_id="marketing_tv_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MANUAL_COST_SHEET_ID,
               "range_name": 'tv_cost',
               "target_table": "marketing_tv_cost",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "country": sqlalchemy.types.VARCHAR(length=65535),
                              "currency": sqlalchemy.types.VARCHAR(length=65535),
                              "brand_non_brand": sqlalchemy.types.VARCHAR(length=65535),
                              "cash_percentage": sqlalchemy.types.VARCHAR(length=65535),
                              "gross_actual_spend": sqlalchemy.types.VARCHAR(length=65535),
                              "gross_budget_spend": sqlalchemy.types.VARCHAR(length=65535),
                              "non_cash_percentage": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_ebay_kleinanzeigen_cost = PythonOperator(
    dag=dag,
    task_id="marketing_ebay_kleinanzeigen_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": EBAY_KLEINANZEIGEN_SHEET_ID,
               "range_name": 'ebay_costs',
               "target_table": "marketing_cost_ebay_kleinanzeigen",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "category": sqlalchemy.types.VARCHAR(length=65535),
                              "platform": sqlalchemy.types.VARCHAR(length=65535),
                              "clicks": sqlalchemy.types.VARCHAR(length=65535),
                              "impressions": sqlalchemy.types.VARCHAR(length=65535),
                              "cost": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_ebay_kleinanzeigen_campaign_cost = PythonOperator(
    dag=dag,
    task_id="marketing_ebay_kleinanzeigen_campaign_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": EBAY_KLEINANZEIGEN_CAMPAIGN_SHEET_ID,
               "range_name": 'ebay_costs',
               "target_table": "marketing_cost_ebay_kleinanzeigen_campaign",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign": sqlalchemy.types.VARCHAR(length=65535),
                              "l1_category": sqlalchemy.types.VARCHAR(length=65535),
                              "l2_category": sqlalchemy.types.VARCHAR(length=65535),
                              "clicks": sqlalchemy.types.VARCHAR(length=65535),
                              "impressions": sqlalchemy.types.VARCHAR(length=65535),
                              "cost": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_yakk_b2b_cost = PythonOperator(
    dag=dag,
    task_id="marketing_yakk_b2b_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": YAKK_B2B_COST_SHEET_ID,
               "range_name": 'yakk_b2b_costs',
               "target_table": "marketing_cost_yakk_b2b",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "order_id": sqlalchemy.types.VARCHAR(length=65535),
                              "total_order_value": sqlalchemy.types.VARCHAR(length=65535),
                              "commission_share": sqlalchemy.types.VARCHAR(length=65535),
                              "paid_commission": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_marktplaats_cost = PythonOperator(
    dag=dag,
    task_id="marketing_marktplaats_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MARKTPLAATS_COST_SHEET_ID,
               "range_name": 'Sheet1',
               "target_table": "marketing_cost_marktplaats",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "cost": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_milanuncios_cost = PythonOperator(
    dag=dag,
    task_id="marketing_milanuncios_cost",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": MILANUNCIOS_COST_SHEET_ID,
               "range_name": 'Sheet1',
               "target_table": "marketing_cost_milanuncios",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "cost": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_apple_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_apple_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "apple_supermetric",
               "target_table": "marketing_apple_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "account": sqlalchemy.types.VARCHAR(length=65535),
                   "installs": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_bing_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_bing_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "bing_supermetric",
               "target_table": "marketing_bing_supermetric",
               "target_schema": "staging",
               "data_types": {"date": sqlalchemy.types.VARCHAR(length=65535),
                              "clicks": sqlalchemy.types.VARCHAR(length=65535),
                              "account": sqlalchemy.types.VARCHAR(length=65535),
                              "conversions": sqlalchemy.types.VARCHAR(length=65535),
                              "impressions": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                              "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                              "advertising_channel_type": sqlalchemy.types.VARCHAR(length=65535),
                              "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
                              }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_facebook_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_facebook_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "facebook_supermetric",
               "target_table": "marketing_facebook_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "clicks": sqlalchemy.types.VARCHAR(length=65535),
                   "account": sqlalchemy.types.VARCHAR(length=65535),
                   "ad_name": sqlalchemy.types.VARCHAR(length=65535),
                   "conversions": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                   "ad_set_name": sqlalchemy.types.VARCHAR(length=65535),
                   "medium": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_google_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_google_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "google_supermetric",
               "target_table": "marketing_google_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "clicks": sqlalchemy.types.VARCHAR(length=65535),
                   "account": sqlalchemy.types.VARCHAR(length=65535),
                   "conversions": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "advertising_channel_type": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_linkedin_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_linkedin_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "linkedin_supermetric",
               "target_table": "marketing_linkedin_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "clicks": sqlalchemy.types.VARCHAR(length=65535),
                   "conversions": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_group_name": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_outbrain_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_outbrain_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "outbrain_supermetric",
               "target_table": "marketing_outbrain_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "clicks": sqlalchemy.types.VARCHAR(length=65535),
                   "conversions": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_snapchat_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_snapchat_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "snapchat_supermetric",
               "target_table": "marketing_snapchat_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "clicks": sqlalchemy.types.VARCHAR(length=65535),
                   "ad_name": sqlalchemy.types.VARCHAR(length=65535),
                   "conversions": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_tiktok_supermetric = PythonOperator(
    dag=dag,
    task_id="marketing_tiktok_supermetric",
    python_callable=load_data_from_gsheet,
    op_kwargs={"conn_id": GOOGLE_CLOUD_CONN,
               "sheet_id": SUPERMETRICS_SHEET_ID,
               "range_name": "tiktok_supermetric",
               "target_table": "marketing_tiktok_supermetric",
               "target_schema": "staging",
               "data_types": {
                   "date": sqlalchemy.types.VARCHAR(length=65535),
                   "clicks": sqlalchemy.types.VARCHAR(length=65535),
                   "ad_name": sqlalchemy.types.VARCHAR(length=65535),
                   "conversions": sqlalchemy.types.VARCHAR(length=65535),
                   "impressions": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_name": sqlalchemy.types.VARCHAR(length=65535),
                   "campaign_id": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_eur": sqlalchemy.types.VARCHAR(length=65535),
                   "total_spent_local_currency": sqlalchemy.types.VARCHAR(length=65535),
               }
               },
    executor_config=EXECUTOR_CONFIG
)

marketing_costs_daily_importer = PythonOperator(
    dag=dag,
    python_callable=run_marketing_costs_daily_importer,
    task_id="marketing_costs_daily_importer",
    executor_config=EXECUTOR_CONFIG
)

marketing_cost_daily_base_data = RunQueryFromRepoOperator(
    dag=dag,
    task_id='marketing_cost_daily_base_data',
    conn_id='redshift_default',
    directory='11_marketing_reporting',
    file='marketing.marketing_cost_daily_base_data',
)

marketing_cost_daily_combined = RunQueryFromRepoOperator(
    dag=dag,
    task_id='marketing_cost_daily_combined',
    conn_id='redshift_default',
    directory='11_marketing_reporting',
    file='marketing.marketing_cost_daily_combined',
)

marketing_cost_daily_reporting = RunQueryFromRepoOperator(
    dag=dag,
    task_id='marketing_cost_daily_reporting',
    conn_id='redshift_default',
    directory='11_marketing_reporting',
    file='marketing.marketing_cost_daily_reporting',
    autocommit=False
)

chain(
    marketing_affiliate_fixed_costs,
    # marketing_affiliate_order_costs,
    marketing_channel_mapping,
    marketing_billboard_cost,
    marketing_criteo_cost,
    marketing_influencer_cost_weekly,
    marketing_partnership_cost,
    marketing_podcasts_cost,
    marketing_reddit_cost,
    marketing_sponsorships_cost,
    marketing_retail_cost,
    marketing_seo_cost,
    marketing_tv_cost,
    marketing_ebay_kleinanzeigen_cost,
    marketing_ebay_kleinanzeigen_campaign_cost,
    marketing_yakk_b2b_cost,
    marketing_marktplaats_cost,
    marketing_milanuncios_cost,
    marketing_apple_supermetric,
    marketing_bing_supermetric,
    marketing_facebook_supermetric,
    marketing_google_supermetric,
    marketing_linkedin_supermetric,
    marketing_outbrain_supermetric,
    marketing_snapchat_supermetric,
    marketing_tiktok_supermetric,
    marketing_costs_daily_importer,
    marketing_cost_daily_base_data,
    marketing_cost_daily_combined,
    marketing_cost_daily_reporting
)
