###
### Preperation
###
# Fresh start / clean workspace
rm(list = ls())

# For Jupyter notebooks only
options(repr.matrix.max.cols=100, repr.matrix.max.rows=60)

## Load packages
suppressPackageStartupMessages({
  library(tidyverse)
  library(tibbletime)
  library(lubridate)
  library(anomalize)
})

# Assign necessary function to functions in '.GlobalEnv' (speed up)
data.table <- data.table::data.table
first_DT <- data.table::first
uniqueN <- data.table::uniqueN
stri_trim_both <- stringi::stri_trim_both

# Custom function for rounding
round_any = function(x, accuracy, f=round){f(x/accuracy) * accuracy}

## Disables messages from dplyr::summarise regarding grouping
options(dplyr.summarise.inform = FALSE)

## Start and end date for all calculations
# start_date equals the 1st day of the month two years ago,
# e.g. 15/12/2020 -> 01/12/2018
start_date <- floor_date(x = today() - years(2), unit = 'month')
end_date <- today()

# Set the global current hour
current_hour <- hour(with_tz(now(), tzone = "CET"))

###
### Retrieve and process data
###

# Sets system environment variables only if in Jupyter environment
if (file.exists('config_airflow.yml')) {
    config <- config::get(file='config_airflow.yml', config='default')
    config_redshift <- config::get(file='config_airflow.yml', config='redshift')
    Sys.setenv(DWH_HOST = config$HOST, DWH_PASSWORD = config$PASSWORD,
               DWH_REDSHIFT_HOST = config_redshift$HOST, DWH_REDSHIFT_PASSWORD = config_redshift$PASSWORD)
}

## 'subscriptions'
HOST <- Sys.getenv("DWH_REDSHIFT_HOST")
NAME <- "dev"
USER <- "risk_connect"
PASSWORD <- Sys.getenv("DWH_REDSHIFT_PASSWORD")
PORT <- "5439"

PG_HOST <-  Sys.getenv("DWH_HOST")
PG_NAME <- "datawarehouse"
PG_USER <- "risk_connect"
PG_PASSWORD <- Sys.getenv("DWH_PASSWORD")
PG_PORT <- "5432"

pconn_r <- DBI::dbConnect(RPostgres::Postgres(),
                          host = HOST,
                          dbname = NAME,
                          user = USER,
                          password = PASSWORD,
                          port = PORT)

QUERY_SUBSCRIPTIONS <- paste('SELECT
    oi.orderitemnumber AS subscription_id,
    o.spree_customer_id__c::int AS customer_id,
    o.spree_order_number__c AS order_id,
    oi.minimum_term_months__c AS committed_duration,
    oi.totalprice * case
        when coalesce(oi.minimum_term_months__c,0)::integer = 0
        then 1
        else coalesce(oi.minimum_term_months__c,1)
        end AS committed_sub_value,
    oi.quantity as item_quantity,
    o.shippingcountry AS shipping_country,
    o.shippingpostalcode AS shipping_pc,
    su.email,
    o.createddate AS order_submission,
    o.payment_method__c AS payment_method,
    oi.brand__c AS brand,
    c.name as subcategory_name,
    p."sku_variant__c" as sku_variant,
    p."sku_product__c" as sku_product,
    p."CreatedDate" as asset_introduction_date
FROM stg_salesforce."orderitem" AS oi
LEFT JOIN stg_salesforce."order" AS o
    ON oi."OrderId" = o."Id"
LEFT JOIN stg_api_production.spree_orders AS so
    ON so."number" = o.spree_order_number__c
LEFT JOIN stg_api_production.spree_users su
    ON su.id = so.user_id
LEFT JOIN stg_salesforce.product2 AS p
     ON p."Id" = oi.product_id__c
LEFT JOIN stg_api_production.categories c
    ON c.permalink = p.category__c
LEFT JOIN stg_api_production.spree_stores AS ss
    ON so.store_id = ss.id
WHERE NOT ss.offline
AND NOT (o.shippingcountry || o.shippingpostalcode || o.shippingstreet like \'Germany10179Holzmarktstr%11\')
AND su.user_type = \'normal_customer\'
AND o.spree_customer_id__c IS NOT NULL
AND o.spree_order_number__c IS NOT NULL
AND NOT (su.email like \'%@grover.com\' or su.email like \'%@getgrover.com\')
AND o.createddate >= ', paste0("\'",start_date,"\';"))

subscriptions <- DBI::dbGetQuery(pconn_r, QUERY_SUBSCRIPTIONS)
DBI::dbDisconnect(pconn_r)
rm(QUERY_SUBSCRIPTIONS)

# Get labels
pconn_r <- DBI::dbConnect(RPostgres::Postgres(),
                          host = HOST,
                          dbname = NAME,
                          user = USER,
                          password = PASSWORD,
                          port = PORT)

QUERY_LABELS <- "SELECT * FROM s3_spectrum_rds_dwh_order_approval.customer_labels3;"

labels <- DBI::dbGetQuery(pconn_r, QUERY_LABELS)
DBI::dbDisconnect(pconn_r)
rm(QUERY_LABELS)

labels <-
    labels %>%
    select(customer_id, label, is_blacklisted) %>%
    mutate(customer_id = as.integer(customer_id),
           label = if_else(is_blacklisted %in% 'True', 'blacklisted', label),
           is_fraud_blacklisted = if_else(label %in% c('good', 'uncertain'), 'False', NA_character_),
           is_fraud_blacklisted = if_else(label %in% c('fraud', 'blacklisted'), 'True', is_fraud_blacklisted))

## Filter 'subscriptions', set proper time / date columns and delete duplicates
subscriptions <-
  subscriptions %>%
  filter(# German addresses have 5-digit zip code
         # Austrian addresses have a 4-digit zip code
         # Dutch addresses have a 7-digit zip code
         # Spanish addresses have a 5-digit zip code
         ((shipping_country %in% 'Germany' & nchar(shipping_pc) %in% 5) |
         (shipping_country %in% 'Austria' & nchar(shipping_pc) %in% 4) |
         (shipping_country %in% 'Netherlands' & nchar(shipping_pc) %in% 7) |
         (shipping_country %in% 'Spain' & nchar(shipping_pc) %in% 5))) %>%
  mutate(order_submission = with_tz(order_submission, tzone = "CET")) %>%
  mutate(# define 'day' as the last 24 hours (current hour + the previous 23 hours)
    day = if_else(hour(order_submission) < current_hour + 1,
                  as_date(order_submission) - 1,
                  as_date(order_submission)),
    time = hour(order_submission)) %>%
  # Use 'day' as 'date' for all later steps (last 24 hours)
  mutate(date = day) %>%
  # Filter out dates before start_date created by re-definition
  filter(date >= start_date) %>%
  # Columns 'subcategory_name' and 'brand' to lowercase
  mutate(subcategory_name = tolower(subcategory_name),
         brand = tolower(brand)) %>%
  distinct()

# Add label column 'is_fraud_blacklisted' to 'subscriptions'
subscriptions <-
  subscriptions %>%
  left_join(labels[, c('customer_id', 'is_fraud_blacklisted')], by = 'customer_id')

## Create time-series data on customer, data & country level
customer_level_ts <-
    data.table(subscriptions)[
        , .(number_subscriptions=uniqueN(subscription_id),
            number_orders = uniqueN(order_id),
            sum_item_quantity = sum(item_quantity, na.rm = TRUE),
            sum_committed_sub_value = sum(committed_sub_value, na.rm = TRUE),
            distinct_payment_methods = uniqueN(payment_method),
            n_phone_and_laptops = sum(item_quantity[subcategory_name %in% c('laptops','smartphones')], na.rm=TRUE),
            n_subs_above_12 = sum(item_quantity[committed_duration >= 12], na.rm=TRUE),
            is_fraud_blacklisted = first_DT(is_fraud_blacklisted)),
            keyby=.(customer_id, date, shipping_country)
    ][date >= start_date & date <= end_date][
        , c('n_subs_above_12', 'duplicate_payment_methods') :=
        .(n_subs_above_12 / number_subscriptions,
          ifelse(distinct_payment_methods == number_subscriptions,
                 0, number_subscriptions - distinct_payment_methods + 1))
    ] %>% tibble()

# Find customer_id & date combination which have committed subscription value above 10,000 and
# exclude them in 'subscriptions' & 'customer_level_ts'
outlier_customer_id_date <-
    customer_level_ts %>%
    ungroup() %>%
    filter(sum_committed_sub_value > 10000) %>%
    select(customer_id, date, sum_committed_sub_value) %>%
    distinct()

subscriptions <-
    subscriptions %>%
    left_join(outlier_customer_id_date, by = c("customer_id", "date")) %>%
    mutate(sum_committed_sub_value = replace_na(sum_committed_sub_value, 0)) %>%
    filter(sum_committed_sub_value <= 10000) %>%
    select(-sum_committed_sub_value)

customer_level_ts <-
    customer_level_ts %>%
    ungroup() %>%
    filter(sum_committed_sub_value <= 10000) %>%
    group_by(customer_id, date, shipping_country)
rm(outlier_customer_id_date)

### Total logins from same session ID data ('multiple_user_id' penalty)
pconn_r <- DBI::dbConnect(RPostgres::Postgres(),
                          host = HOST,
                          dbname = NAME,
                          user = USER,
                          password = PASSWORD,
                          port = PORT)

QUERY_userid_session_id_matching <- 'select user_id, creation_time, user_id2 from machine_learning.userid_session_id_matching;'

userid_session_id_matching <- DBI::dbGetQuery(pconn_r, QUERY_userid_session_id_matching)
userid_session_id_matching <- userid_session_id_matching %>% filter(user_id %in% subscriptions$customer_id)
DBI::dbDisconnect(pconn_r)
rm(QUERY_userid_session_id_matching)

# Create a day (date) column which mirrors the 'date' column from subscriptions
# make 'user_id.glv2' distinct for each 'user_id' (i.e. only the first entry is kept),
# Filter out customer_ids not in 'subscriptions'
# Count distinct 'user_id.glv2' per user_id & date,
# Calculate (cumultive) sum of 'distinct_user_id2'
# ungroup
total_logins_dates <-
  userid_session_id_matching %>%
  filter(user_id %in% subscriptions$customer_id) %>%
  mutate(creation_time = with_tz(creation_time, tzone = "CET")) %>%
  # define 'day' as the last 24 hours (current hour + the previous 23 hours)
  mutate(day = if_else(hour(creation_time) < current_hour + 1,
                       as_date(creation_time) - 1,
                       as_date(creation_time))) %>%
  select(user_id, user_id2, day) %>%
  group_by(user_id) %>%
  arrange(day, user_id, user_id2) %>%
  distinct(user_id, user_id2, .keep_all = TRUE) %>%
  group_by(user_id, day) %>%
  summarise(distinct_user_id2 = n_distinct(user_id2)) %>%
  group_by(user_id) %>%
  mutate(unique_user_id2 = cumsum(distinct_user_id2)) %>%
  ungroup()
rm(userid_session_id_matching)

# Get unique combination of customer_id & day from subscriptions (.sub),
# left join with 'total_logins_dates' (.tld) by customer_id, this will match each combination of
# customer_id + day.sub with all days from tld for that customer_id
# filter out rows where day.tld > day.sub
# filter out all day.tld rows except the closest row (by day)
# ungroup and rename
total_logins_dates <-
  subscriptions %>%
  select(customer_id, day) %>%
  distinct() %>%
  left_join(total_logins_dates, by = c('customer_id' = 'user_id'), suffix = c('.sub', '.tld')) %>%
  mutate(diff_days = difftime(day.sub, day.tld, unit="days")) %>%
  filter(diff_days >= 0) %>%
  group_by(customer_id, day.sub) %>%
  slice_min(order_by = diff_days, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(customer_id, day.sub, unique_user_id2) %>%
  rename(day = day.sub) %>%
  arrange(customer_id, day)

### asset risk
# Convert column values and create new columns for later steps
# - Convert values of subcategory_name, brand & sku_product to lowercase and remove leading and trailing whitespaces
# - Create asset_introduction_date column and `years_since_asset_intro`
#     - Possible values for years_since_asset_intro: 0, 1, 2 years
# - For special cases (Apple smartphones & laptops, Samsung smartphones)
#     - Create `storage_space` column (e.g. 64gb, 128gb, ..., 512gb)
#     - Create `core_version` column (e.g. i3, i5, i7, i9)
subscriptions <-
    subscriptions %>%
    mutate(subcategory_name = stri_trim_both(tolower(subcategory_name), pattern = "\\P{Wspace}"),
           brand = stri_trim_both(tolower(brand), pattern = "\\P{Wspace}")) %>%
    mutate(asset_introduction_date = as_date(with_tz(asset_introduction_date, tzone = "CET")),
           days_since_asset_intro = difftime(as_date(order_submission), asset_introduction_date, unit="days"),
           days_since_asset_intro = as.numeric(days_since_asset_intro),
           years_since_asset_intro = if_else(days_since_asset_intro <= 365, 0, as.double(NA_integer_)),
           years_since_asset_intro = if_else(days_since_asset_intro > 365, 1, years_since_asset_intro),
           years_since_asset_intro = if_else(days_since_asset_intro > (365 * 2), 2, years_since_asset_intro)) %>%
    select(-days_since_asset_intro)

pconn_r <- DBI::dbConnect(RPostgres::Postgres(),
                          host = HOST,
                          dbname = NAME,
                          user = USER,
                          password = PASSWORD,
                          port = PORT)

# Query 'stg_fraud_and_credit_risk.asset_risk_categories'
QUERY_asset_risk_categories <- 'select * from stg_fraud_and_credit_risk.asset_risk_categories;'

asset_risk_categories <- DBI::dbGetQuery(pconn_r, QUERY_asset_risk_categories)
asset_risk_categories <- asset_risk_categories %>% select(-id, -created_at)
DBI::dbDisconnect(pconn_r)
rm(QUERY_asset_risk_categories)

# Fix inconsistent data problems
# - Only one `brand` value per `sku_product` value
# - Only one `subcategory_name` value per `sku_product` value
brand_max_sku_product <- 
    asset_risk_categories %>% 
    select(brand, sku_product) %>% 
    distinct()

subcategory_name_max_sku_product <- 
    asset_risk_categories %>% 
    select(subcategory_name, sku_product) %>% 
    distinct()

# Get model attributes per sku_variant + sku_product
sku_max_model_attribute <- 
    asset_risk_categories %>% 
    select(sku_variant, sku_product, special_attribute) %>% 
    distinct()

# Get subcategories for which the asset age is significant
subcategories_asset_age_significant <- 
    asset_risk_categories %>% 
    filter(!years_since_asset_intro %in% -9) %>% 
    pull(subcategory_name) %>% 
    unique()

# - Fix data inconsistencies in 'subscriptions'
# - Add country_code
# - Set 'years_since_asset_intro' to -9 if subcategory not in 'subcategories_asset_age_significant'
subscriptions <- 
    subscriptions %>%  
    left_join(brand_max_sku_product, by = 'sku_product', suffix = c('_old', '')) %>% 
    left_join(subcategory_name_max_sku_product, by = 'sku_product', suffix = c('_old', '')) %>%
    left_join(sku_max_model_attribute, by = c('sku_variant', 'sku_product'), suffix = c('_old', '')) %>%
    mutate(brand = if_else(is.na(brand), brand_old, brand),
           subcategory_name = if_else(is.na(subcategory_name), subcategory_name_old, subcategory_name)) %>% 
    select(-brand_old, -subcategory_name_old) %>% 
    mutate(country_code = if_else(shipping_country %in% 'Germany', 'DE', NA_character_),
           country_code = if_else(shipping_country %in% 'Austria', 'AT', country_code),
           country_code = if_else(shipping_country %in% 'Netherlands', 'NL', country_code),
           country_code = if_else(shipping_country %in% 'Spain', 'ES', country_code),
           years_since_asset_intro = if_else(!subcategory_name %in% subcategories_asset_age_significant,
                                             -9, years_since_asset_intro))
rm(brand_max_sku_product, subcategory_name_max_sku_product,
   sku_max_model_attribute, subcategories_asset_age_significant)

# Create separate DFs from 'asset_risk_categories' to allow proper joins with 'subscriptions'
asset_risk_categories_uses_sku <- 
    asset_risk_categories %>% 
    filter(type %in% c('uses_sku_product', 'uses_sku_variant')) %>% 
    select(country_code, subcategory_name, brand, sku_product, sku_variant, 
           years_since_asset_intro, special_attribute, numeric_fraud_rate) %>% 
    distinct()

asset_risk_categories_uses_brand_subcategory <- 
    asset_risk_categories %>% 
    filter(type %in% 'uses_brand_subcategory') %>% 
    select(country_code, subcategory_name, brand, 
           years_since_asset_intro, special_attribute, numeric_fraud_rate) %>% 
    distinct()

asset_risk_categories_uses_subcategory <- 
    asset_risk_categories %>% 
    filter(type %in% 'uses_subcategory') %>% 
    select(country_code, subcategory_name,  
           years_since_asset_intro, special_attribute, numeric_fraud_rate) %>% 
    distinct()

# Add correct type of 'numeric_fraud_rate' to 'subscriptions' 
subscriptions <- 
    subscriptions %>% 
    left_join(asset_risk_categories_uses_sku,
              by = c("country_code", "subcategory_name", "brand", "sku_product",
                     "sku_variant", "years_since_asset_intro", "special_attribute")) %>% 
    left_join(asset_risk_categories_uses_brand_subcategory,
              by = c("country_code", "subcategory_name", "brand",
                     "years_since_asset_intro", "special_attribute"),
              suffix= c("", "_brand_subcategory")) %>% 
    left_join(asset_risk_categories_uses_subcategory,
              by = c("country_code", "subcategory_name",
                     "years_since_asset_intro", "special_attribute"),
              suffix= c("", "_subcategory"))
rm(asset_risk_categories_uses_sku, 
   asset_risk_categories_uses_brand_subcategory,
   asset_risk_categories_uses_subcategory)

# Replace NAs of 'numeric_fraud_rate' by '_brand_subcategory' or '_subcategory' value
# Remove columns which are redudant now and rename 'numeric_fraud_rate' to
# 'asset_risk_category_fraud_rate'
subscriptions <- 
    subscriptions %>% 
    mutate(numeric_fraud_rate = if_else(is.na(numeric_fraud_rate), 
                                        numeric_fraud_rate_brand_subcategory, numeric_fraud_rate),
           numeric_fraud_rate = if_else(is.na(numeric_fraud_rate), 
                                        numeric_fraud_rate_subcategory, numeric_fraud_rate)) %>% 
    select(-numeric_fraud_rate_brand_subcategory, -numeric_fraud_rate_subcategory,
           -sku_variant, -sku_product, -asset_introduction_date,
           -years_since_asset_intro, -brand, -subcategory_name, -special_attribute) %>% 
    rename(asset_risk_category_fraud_rate = numeric_fraud_rate)

### Get order_payment data from order_approval.order_payment for
### (also from order_approval.neutrino_card_bin)

# Query DB
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                      host = PG_HOST,
                      dbname = PG_NAME,
                      user = PG_USER,
                      password = PG_PASSWORD,
                      port = PG_PORT)

QUERY_order_payment <- "SELECT customer_id, order_id,
                        payment_method, payment_type,
                        payment_metadata ->> 'paypal_verified' as paypal_verified,
                        payment_metadata ->> 'paypal_email' as paypal_email,
                        payment_metadata ->> 'credit_card_bank_name' as credit_card_bank_name,
                        ncb.card_issuer
                        FROM order_approval.order_payment op
                        left join order_approval.neutrino_card_bin ncb on
                        ncb.card_bin_number = op.payment_metadata ->> 'credit_card_bin';"

order_payment <- DBI::dbGetQuery(con, QUERY_order_payment)
order_payment <- order_payment %>% filter(order_id %in% subscriptions$order_id)
DBI::dbDisconnect(con)
rm(QUERY_order_payment)

# Replace empty string with NA; Remove leading/trailing quotes from string
# Combine `card_issuer` (1st choice) & `credit_card_bank_name` (2nd choice)
order_payment <-
    order_payment %>%
    mutate(card_issuer = if_else(card_issuer %in% '', NA_character_, card_issuer),
           card_issuer = stringi::stri_replace_all_fixed(str = card_issuer, pattern = '"', replacement = ''),
           credit_card_bank_name = stringi::stri_replace_all_fixed(str = credit_card_bank_name,  pattern = '"',
                                                                   replacement = ''),
           paypal_verified = stringi::stri_replace_all_fixed(str = paypal_verified, pattern = '"',
                                                                   replacement = '')) %>%
    mutate(card_issuer = if_else(card_issuer %in% c(NA, 'NA', 'null') & !is.na(credit_card_bank_name),
                                 credit_card_bank_name, card_issuer)) %>%
    select(-credit_card_bank_name)

# Create mapping list (like a dictionary) to combine similarly spelled bank names which represent the same entity
card_issuer_mappings <- list(
    `ALLIED IRISH BANKS PLC` = 'ALLIED IRISH BANKS PLC',
    `ALLIED IRISH BANKS, PLC` = 'ALLIED IRISH BANKS PLC',
    `AXIS BANK LTD` = 'AXIS BANK LTD',
    `AXIS BANK, LTD.` = 'AXIS BANK LTD',
    `BANCO BILBAO VIZCAYA ARGENTARIA, S.A.`= 'BANCO BILBAO VIZCAYA ARGENTARIA, S.A.',
    `BANCO BILBAO VIZCAYA ARGENTARIA S.A. (BBVA)`= 'BANCO BILBAO VIZCAYA ARGENTARIA, S.A.',
    `BANK OF AMERICA N.A.` = 'BANK OF AMERICA N.A.',
    `BANK OF AMERICA, N.A.` = 'BANK OF AMERICA N.A.',
    `BANK OF AMERICA, NATIONAL ASSOCIATION` = 'BANK OF AMERICA N.A.',
    `BANK POLSKA KASA OPIEKI S.A. (BANK PEKAO SA)` = 'BANK POLSKA KASA OPIEKI S.A. (BANK PEKAO SA)',
    `BANK POLSKA KASA OPIEKI S.A. - (BANK PEKAO S.A.)` = 'BANK POLSKA KASA OPIEKI S.A. (BANK PEKAO SA)',
    `BARCLAYS BANK PLC` = 'BARCLAYS BANK PLC',
    `BARCLAYS BANK PLC.` = 'BARCLAYS BANK PLC',
    `BRD GROUPE SOCIETE GENERALE` = 'BRD GROUPE SOCIETE GENERALE',
    `BRD - GROUPE SOCIETE GENERALE, S.A.` = 'BRD GROUPE SOCIETE GENERALE',
    `CAIXA GERAL DE DEPOSITOS SA` = 'CAIXA GERAL DE DEPOSITOS SA',
    `CAIXA GERAL DE DEPOSITOS, S.A.` = 'CAIXA GERAL DE DEPOSITOS SA',
    `CAPITAL ONE BANK (USA), N.A.` = 'CAPITAL ONE BANK (USA), N.A.',
    `CAPITAL ONE BANK (USA), NATIONAL ASSOCIATION` = 'CAPITAL ONE BANK (USA), N.A.',
    `DKB` = 'DEUTSCHE KREDITBANK AG',
    `DEUTSCHE KREDITBANK AG` = 'DEUTSCHE KREDITBANK AG',
    `DEUTSCHE KREDITBANK AKTIENGESELLSCHAFT` = 'DEUTSCHE KREDITBANK AG',
    `DEUTSCHER SPARKASSEN UND GIROVERBAND` = 'DEUTSCHER SPARKASSEN UND GIROVERBAND',
    `DEUTSCHER SPARKASSEN- UND GIROVERBAND` = 'DEUTSCHER SPARKASSEN UND GIROVERBAND',
    `DNB BANK ASA` = 'DNB BANK ASA',
    `DNB NOR BANK ASA` = 'DNB BANK ASA',
    `ERSTE & STEIERMARKISCHE BANK D.D.` = 'ERSTE & STEIERMARKISCHE BANK D.D.',
    `ERSTE AND STEIERMARKISCHE BANK D.D.` = 'ERSTE & STEIERMARKISCHE BANK D.D.',
    `ERSTE BANK DER OESTERREICHISCHEN SPARKASSEN AG` = 'ERSTE BANK DER OESTERREICHISCHEN SPARKASSEN AG',
    `ERSTE BANK DER OSTERREICHISCHEN SPARKASSEN AG` = 'ERSTE BANK DER OESTERREICHISCHEN SPARKASSEN AG',
    `GETIN NOBLE BANK` = 'GETIN NOBLE BANK',
    `GETIN NOBLE BANK, S.A.` = 'GETIN NOBLE BANK',
    `HASPA HAMBURGER SPARKASSE` = 'HASPA HAMBURGER SPARKASSE',
    `HASPA BANK` = 'HASPA HAMBURGER SPARKASSE',
    `INTESA SANPAOLO S.P.A.` = 'INTESA SANPAOLO S.P.A.',
    `INTESA SANPAOLO SPA` = 'INTESA SANPAOLO S.P.A.',
    `POWSZECHNA KASA OSZCZEDNOSCI BANK POLSKI SA (PKO BANK POLSKI SA)` = 'POWSZECHNA KASA OSZCZEDNOSCI BANK POLSKI SA (PKO BANK POLSKI SA)',
    `POWSZECHNA KASA OSZCZEDNOSCI BANK POLSKI S.A. (PKO BANK POLSKI S.A.)` = 'POWSZECHNA KASA OSZCZEDNOSCI BANK POLSKI SA (PKO BANK POLSKI SA)',
    `RAIFFEISEN BANK INTERNATIONAL AG` = 'RAIFFEISEN BANK INTERNATIONAL AG',
    `RAIFFEISEN BANK INTERNATIONAL` = 'RAIFFEISEN BANK INTERNATIONAL AG',
    `TURKIYE GARANTI BANKASI A. S.` = 'TURKIYE GARANTI BANKASI, A.S.',
    `TURKIYE GARANTI BANKASI, A.S.` = 'TURKIYE GARANTI BANKASI, A.S.',
    `UNICREDIT BULBANK AD` = 'UNICREDIT BULBANK AD',
    `UNICREDIT BULBANK A.D.` = 'UNICREDIT BULBANK AD',
    `WUESTENROT BANK AG PFANDBRIEBANK` = 'WUESTENROT BANK AG PFANDBRIEBANK',
    `WUSTENROT BANK AG` = 'WUESTENROT BANK AG PFANDBRIEBANK')

# Map new bank names to 'card_issuer_new' column
order_payment$card_issuer_new <- NA_character_
order_payment$card_issuer_new[order_payment$card_issuer %in% names(card_issuer_mappings)] <-
    sapply(X = order_payment$card_issuer[order_payment$card_issuer %in% names(card_issuer_mappings)],
           function(x) card_issuer_mappings[[x]])
rm(card_issuer_mappings)

# Overwrite 'card_issuer' column with 'card_issuer_new' where new names where mapped in the previous step; remove 'card_issuer_new'
order_payment <-
    order_payment %>%
     mutate(card_issuer = if_else(!is.na(card_issuer) & !is.na(card_issuer_new), card_issuer_new, card_issuer)) %>%
     select(-card_issuer_new)

subscriptions <-
    subscriptions %>%
    left_join(order_payment, by = c('customer_id', 'order_id'), suffix = c('', '.op'))
rm(order_payment)

# Create `payment_category_penalty`
# New values for `paypal_verified` & `card_issuer` for their null & NA values as we have many and they change over time
# `payment_category_penalty`: Use `paypal_verified` or `card_issuer` if possible
# Assign value for `payment_category_penalty` based on `payment_type` & `payment_method.op` for remaining values
subscriptions <-
    subscriptions %>%
    mutate(paypal_verified = if_else(payment_method.op %in% 'PayPal' & paypal_verified %in% 'null',
                                     'null_PayPal', paypal_verified),
           paypal_verified = if_else(payment_method.op %in% 'paypal-gateway' &
                                     paypal_verified %in% 'null' &
                                     date < ymd('2020-10-01'),
                                     paste('null',year(date),paste0('q', quarter(date)), sep='_'),
                                     paypal_verified),
           paypal_verified = if_else(paypal_verified %in% c('false'), 'paypal_verified_false', paypal_verified),
           paypal_verified = if_else(paypal_verified %in% c('true'), 'paypal_verified_true', paypal_verified),
           paypal_verified = if_else(paypal_verified %in% c('null'), 'paypal_verified_null', paypal_verified),
           paypal_verified = if_else(payment_method.op %in% 'PayPal' & is.na(paypal_verified),
                                     paste('NA', payment_method.op, sep = '_'), paypal_verified),
           card_issuer = if_else(!payment_method.op %in% 'paypal-gateway' &
                                 card_issuer %in% 'null' & date < ymd('2020-05-29'),
                                 paste('null_credit_card', year(date), paste0('q', quarter(date)), sep = '_'),
                                 card_issuer),
           card_issuer = if_else(!tolower(payment_type) %in% 'paypal' & card_issuer %in% c('null', '"NA"'),
                                 'null_credit_card', card_issuer)) %>%
    mutate(payment_category_penalty = if_else(tolower(payment_type) %in% 'paypal',
                                              paypal_verified, NA_character_),
           payment_category_penalty = if_else(!tolower(payment_type) %in% 'paypal',
                                              card_issuer, payment_category_penalty),
           payment_category_penalty = if_else(is.na(payment_category_penalty) &
                                              is.na(payment_type) &
                                              is.na(payment_method.op),
                                              'NA_order_not_in_order_payment_table', payment_category_penalty),
           payment_category_penalty = if_else(is.na(payment_category_penalty) &
                                              payment_type %in% 'credit_card' &
                                              payment_method.op %in% 'AdyenContract',
                                              'NA_cc_AdyenContract', payment_category_penalty),
           payment_category_penalty = if_else(is.na(payment_category_penalty) &
                                              payment_type %in% 'sepa' &
                                              payment_method.op %in% 'sepa-gateway',
                                              'NA_sepa_sepa-gateway', payment_category_penalty),
           payment_category_penalty = replace_na(payment_category_penalty, 'NA_other')) %>%
    select(-c('payment_method.op', 'payment_type', 'paypal_verified'))

# Calculate overall (all countries) `is_fraud_blacklisted_ratio` for each `payment_category_penalty` (some are `NaN`)
payment_category_penalty_all_countries_ratio <-
    subscriptions %>%
    group_by(payment_category_penalty, is_fraud_blacklisted) %>%
    summarise(sum_item_quantity = sum(item_quantity)) %>%
    pivot_wider(payment_category_penalty, names_from = is_fraud_blacklisted, values_from = sum_item_quantity, values_fill = 0) %>%
    ungroup() %>%
    mutate(is_fraud_blacklisted_ratio = True / (False + True)) %>%
    mutate_if(is.double, round, 4)

# Add 'is_fraud_blacklisted_ratio' to 'subscriptions'
subscriptions <-
    subscriptions %>%
    left_join(payment_category_penalty_all_countries_ratio, by = 'payment_category_penalty')
rm(payment_category_penalty_all_countries_ratio)

# Calculate country's `is_fraud_blacklisted_ratio` for each `payment_category_penalty` (some are `NaN`)
payment_category_penalty_one_country_ratio <-
    subscriptions %>%
    group_by(shipping_country, payment_category_penalty, is_fraud_blacklisted) %>%
    summarise(sum_item_quantity = sum(item_quantity)) %>%
    pivot_wider(c(shipping_country, payment_category_penalty), names_from = is_fraud_blacklisted,
                values_from = sum_item_quantity, values_fill = 0) %>%
    ungroup() %>%
    mutate(is_fraud_blacklisted_ratio = True / (False + True)) %>%
    mutate_if(is.double, round, 4)

# Calculate median over all payments and only for card_issuer ones
stats_payment_category <-
    subscriptions %>%
    mutate(is_card_issuer = payment_category_penalty %in% card_issuer) %>%
    left_join(select(payment_category_penalty_one_country_ratio, shipping_country, payment_category_penalty,
                     is_fraud_blacklisted_ratio),
              by = c('shipping_country', 'payment_category_penalty'), suffix = c('', '.country')) %>%
    group_by(shipping_country) %>%
    summarise(overall_median_is_fraud_blacklisted_ratio =
                  median(is_fraud_blacklisted_ratio.country, na.rm = TRUE),
              card_issuer_median_is_fraud_blacklisted_ratio =
                 median(is_fraud_blacklisted_ratio.country[is_card_issuer %in% TRUE], na.rm = TRUE))

# Fill NaN values from `card_issuer` with its median value
# All other NaN / NA values are filled with the median of `overall_is_fraud_blacklisted_ratio`
# `payment_penalty_category` which have at least one labels but not more than 5 labels & not any `True`
# (i.e. fraud & blacklisted) label will use
# 'median_overall_is_fraud_blacklisted_ratio' * (1 / False) (e.g. 1 / 5 if there are 5 False labels)
payment_category_penalty_one_country_ratio <-
    payment_category_penalty_one_country_ratio %>%
    left_join(stats_payment_category, by = 'shipping_country') %>%
    mutate(is_fraud_blacklisted_ratio = if_else(is.na(is_fraud_blacklisted_ratio) &
                                                 payment_category_penalty %in% subscriptions$card_issuer,
                                                 card_issuer_median_is_fraud_blacklisted_ratio,
                                                 is_fraud_blacklisted_ratio),
           is_fraud_blacklisted_ratio = if_else(is.na(is_fraud_blacklisted_ratio),
                                                overall_median_is_fraud_blacklisted_ratio,
                                                is_fraud_blacklisted_ratio),
           is_fraud_blacklisted_ratio = if_else(False > 0 & True < 1 & False <= 5,
                                                overall_median_is_fraud_blacklisted_ratio * (1 / False),
                                                is_fraud_blacklisted_ratio))

# Create character vector of countries with insufficient data
countries_without_sufficient_payment_data <-
    payment_category_penalty_one_country_ratio %>%
    group_by(shipping_country) %>%
    summarise(False_sum = sum(False), True_sum = sum(True)) %>%
    filter(False_sum + True_sum < 100) %>%
    pull(shipping_country)

#
shipping_country_differences <-
    payment_category_penalty_one_country_ratio %>% # country
    left_join(distinct(select(subscriptions, payment_category_penalty, is_fraud_blacklisted_ratio)),
              by = 'payment_category_penalty', suffix = c('.country', '')) %>%
    mutate(is_fraud_blacklisted_ratio.country = replace_na(is_fraud_blacklisted_ratio.country, 0),
           # If no label yet use median (from 'payment_category_penalty_one_country_ratio' in last step)
           # Exception: if shipping_country has not yet sufficient data, use ratio over all countries
           is_fraud_blacklisted_ratio = if_else((True + False) < 1 & !shipping_country %in% countries_without_sufficient_payment_data,
                                                is_fraud_blacklisted_ratio.country, is_fraud_blacklisted_ratio),
           # Due to the exception above, we need to add an extra step to replace NAs
           is_fraud_blacklisted_ratio = replace_na(is_fraud_blacklisted_ratio, 0),
           # As in 'payment_category_penalty_one_country_ratio'
           is_fraud_blacklisted_ratio = if_else(False > 0 & True < 1 & False <= 5,
                                                is_fraud_blacklisted_ratio.country, is_fraud_blacklisted_ratio),
           # If more than 10 labels for a country and the country's fraud ratio is LOWER, use the lower ratio
           is_fraud_blacklisted_ratio = if_else(False + True >= 10 &
                                                is_fraud_blacklisted_ratio.country < is_fraud_blacklisted_ratio,
                                                is_fraud_blacklisted_ratio.country, is_fraud_blacklisted_ratio),
           # If more than 3 labels for a country and the country's fraud ratio is HIGHER, use the higher ratio
           is_fraud_blacklisted_ratio = if_else(False + True >= 3 &
                                                is_fraud_blacklisted_ratio.country > is_fraud_blacklisted_ratio,
                                                is_fraud_blacklisted_ratio.country,
                                                is_fraud_blacklisted_ratio)) %>%
    select(shipping_country, payment_category_penalty,
           False, True, is_fraud_blacklisted_ratio, is_fraud_blacklisted_ratio.country)
rm(countries_without_sufficient_payment_data)

# Only use unupdated ratio if 'is_fraud_blacklisted_ratio.update' is NA
subscriptions <-
    subscriptions %>%
    left_join(shipping_country_differences, by = c('shipping_country', 'payment_category_penalty'),
              suffix = c('', '.update')) %>%
    mutate(is_fraud_blacklisted_ratio = if_else(is.na(is_fraud_blacklisted_ratio.update),
                                                is_fraud_blacklisted_ratio,
                                                is_fraud_blacklisted_ratio.update)) %>%
    select(-False, -True, -`NA`)
rm(shipping_country_differences, payment_category_penalty_one_country_ratio, stats_payment_category)

# For order IDs from the last three days,
order_id_payment_risk_categories <-
    subscriptions %>%
    filter(date >= today() - days(3)) %>%
    # If the two values are different, the 'overall' ratio is used
    mutate(ship_c = if_else(abs(is_fraud_blacklisted_ratio.country - is_fraud_blacklisted_ratio) > 0.001,
                            'overall', NA_character_),
           ship_c = if_else(is.na(ship_c) & shipping_country %in% 'Germany', 'DE', ship_c),
           ship_c = if_else(is.na(ship_c) & shipping_country %in% 'Austria', 'AT', ship_c),
           ship_c = if_else(is.na(ship_c) & shipping_country %in% 'Netherlands', 'NL', ship_c),
           ship_c = if_else(is.na(ship_c) & shipping_country %in% 'Spain', 'ES', ship_c)) %>%
    # Define five categories
    mutate(is_fraud_blacklisted_ratio = if_else(ship_c %in% 'overall', is_fraud_blacklisted_ratio.update,
                                                is_fraud_blacklisted_ratio)) %>%
    mutate(payment_category_fraud_rate = if_else(is_fraud_blacklisted_ratio <= 0.50, '>20%', '>50%'),
           payment_category_fraud_rate = if_else(is_fraud_blacklisted_ratio <= 0.20, '>10%',
                                                 payment_category_fraud_rate),
           payment_category_fraud_rate = if_else(is_fraud_blacklisted_ratio <= 0.10, '>5%',
                                                 payment_category_fraud_rate),
           payment_category_fraud_rate = if_else(is_fraud_blacklisted_ratio <= 0.05, '<5%',
                                                 payment_category_fraud_rate)) %>%
    # Cut off 'payment_category_penalty' after 50 characters
    mutate(payment_category_penalty = if_else(nchar(payment_category_penalty) <= 50,
                                              payment_category_penalty,
                                              paste0(substr(payment_category_penalty, 1, 50), '...'))) %>%
    mutate(fraud_good = paste(False.update, True.update, sep = ' - '),
           payment_category_info = paste(paste0(format(round(is_fraud_blacklisted_ratio * 100, 1),
                                                       nsmall = 1), "%"),
                                         fraud_good, ship_c, payment_category_penalty, sep = ' | ')) %>%
    select(customer_id, order_id, payment_category_fraud_rate, payment_category_info) %>%
    distinct()

write.csv(order_id_payment_risk_categories, 'order_id_payment_risk_categories.csv')
rm(order_id_payment_risk_categories)

# Remove unnecessary columns
subscriptions <-
    subscriptions %>%
    select(-card_issuer, -False.update, -True.update, -is_fraud_blacklisted_ratio.update,
           -is_fraud_blacklisted_ratio.country)

### 'nethone_worst_signal'

pconn_r <- DBI::dbConnect(RPostgres::Postgres(),
                          host = HOST,
                          dbname = NAME,
                          user = USER,
                          password = PASSWORD,
                          port = PORT)

QUERY_nethone_signal_customer_history <- 'select customer_id, order_id, worst_signal as nethone_worst_signal
                                          from data_science_dev.nethone_signal_customer_history;'
QUERY_nethone_signal_risk_categories_order <- 'select customer_id, order_id, signals as nethone_worst_signal
                                               from data_science_dev.nethone_signal_risk_categories_order;'

nethone_signal_customer_history <- DBI::dbGetQuery(pconn_r, QUERY_nethone_signal_customer_history)
nethone_signal_risk_categories_order <- DBI::dbGetQuery(pconn_r, QUERY_nethone_signal_risk_categories_order)
DBI::dbDisconnect(pconn_r)

# Add (newer) `nethone_signal_risk_categories_order` orders, delete duplicates
nethone_signal_customer_history <-
    nethone_signal_risk_categories_order %>%
    bind_rows(nethone_signal_customer_history) %>%
    distinct(customer_id, order_id, .keep_all = TRUE)

# Add column 'nethone_worst_signal'
subscriptions <-
    subscriptions %>%
    left_join(nethone_signal_customer_history, by = c('customer_id', 'order_id')) %>%
    # Signals before introduction of Nethone singals are 'no_signal'
    # Signals after that date are also 'signals_na'
    mutate(nethone_worst_signal = if_else(order_submission >= ymd('20200401') & is.na(nethone_worst_signal),
                                          'no_signal', nethone_worst_signal),
           nethone_worst_signal = replace_na(nethone_worst_signal, 'signals_na'))
rm(nethone_signal_risk_categories_order, nethone_signal_customer_history)

### Historical Labels
pconn_r <- DBI::dbConnect(RPostgres::Postgres(),
                          host = HOST,
                          dbname = NAME,
                          user = USER,
                          password = PASSWORD,
                          port = PORT)

query_customer_labels_state_changes <- "select customer_id::int, label_state, is_blacklisted, updated_at
                                        from s3_spectrum_rds_dwh_order_approval.customer_labels_state_changes3"

customer_labels_state_changes <- DBI::dbGetQuery(pconn_r, query_customer_labels_state_changes)
DBI::dbDisconnect(pconn_r)
rm(pconn_r)

hist_labels <-
    customer_labels_state_changes %>%
    # Overwrite 'label_state' if 'is_blacklisted' is true
    mutate(is_blacklisted = is_blacklisted %in% 'True',
           label_state = if_else(is_blacklisted, 'blacklisted', label_state)) %>%
    # Currently we only keep customers with 'good' label
    filter(label_state %in% c('good', 'uncertain')) %>%
    # define 'day' as the last 24 hours (current hour + the previous 23 hours)
    mutate(updated_at = with_tz(updated_at, tzone = "CET"),
           label_date = if_else(hour(updated_at) < current_hour + 1,
                                as_date(updated_at) - 1,
                                as_date(updated_at))) %>%
    # Only keep customer IDs which appear in 'subscriptions' after the min of 'label_date'
    filter(customer_id %in% subscriptions$customer_id[subscriptions$day >= min(label_date)]) #%>%
 rm(customer_labels_state_changes)


# Join 'subscriptions' &  'hist_labels'; find closest date & time match
hist_labels <-
    subscriptions %>%
    filter(customer_id %in% hist_labels$customer_id,
           day >= min(hist_labels$label_date)) %>%
    select(customer_id, day, order_submission) %>%
    distinct() %>%
    left_join(hist_labels, by = c('customer_id')) %>%
    mutate(diff_times = difftime(order_submission, updated_at, unit="mins")) %>%
    filter(diff_times >= 0) %>%
    group_by(customer_id, day) %>%
    slice_min(order_by = diff_times, n = 1, with_ties = FALSE) %>%
    select(customer_id, day, label_state) %>%
    ungroup()

### Onfido verified - Get customer IDs with recent orders and check if they're already Onfido verified
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                          host = PG_HOST,
                          dbname = PG_NAME,
                          user = PG_USER,
                          password = PG_PASSWORD,
                          port = PG_PORT)

recent_customer_id <-
    subscriptions %>%
    filter(date >= today() - 8) %>%
    pull(customer_id) %>%
    unique()

query_id_verification <- "select user_id as customer_id, curr_state from order_approval.id_verification
                          where curr_state = 'verified'"

recent_customers_onfido_verified <- DBI::dbGetQuery(con, query_id_verification)
recent_customers_onfido_verified <- recent_customers_onfido_verified %>% filter(customer_id %in% recent_customer_id)
DBI::dbDisconnect(con)
rm(recent_customer_id, query_id_verification)

### Save relevant variables for processing of individual countries
# Delete all 'QUERY...' variables from workspace
rm(list = ls()[grepl(pattern = '^QUERY', x = ls(), ignore.case = TRUE)])
# Delete more unneeded variables from workspace
rm(con, current_hour)

# Save entire workspace to disk for use in the individual country scripts
save.image('prep_data_all_countries.RData')


################
### Germany ####
################

###
### Preperation
###
# Fresh start / clean workspace
rm(list = ls())

###
### Load data from preparation script and adjust for Germany
###
load('prep_data_all_countries.RData')

# Only keep data from Germany
subscriptions <-
    subscriptions %>%
    filter(shipping_country %in% 'Germany',
           date >= start_date) %>%
    select(-shipping_country)

# Only keep data from Germany
total_logins_dates <-
    total_logins_dates %>%
    filter(customer_id %in% subscriptions$customer_id,
           day >= start_date)

# Only keep data from Germany; change grouping to 'customer_id' & 'date'
customer_level_ts <-
    customer_level_ts %>%
    ungroup() %>%
    filter(shipping_country %in% 'Germany',
           date >= start_date) %>%
    select(-shipping_country) %>%
    group_by(customer_id, date)

###
### Calculate all metrics for anomaly_score (Germany)
###

# Calculate the means of the metrics used
tibbletime_metrics_means <-
    data.table(customer_level_ts)[date >= start_date & date <= end_date][
        , .(mean_sum_committed_sub_value = mean(sum_committed_sub_value, na.rm=TRUE),
            mean_amount_orders_day = mean(number_orders, na.rm=TRUE),
            mean_sum_item_quantity = mean(sum_item_quantity, na.rm=TRUE),
            mean_duplicate_pm = mean(duplicate_payment_methods, na.rm=TRUE),
            mean_multiple_12 = mean(n_subs_above_12, na.rm=TRUE),
            mean_multiple_pl = mean(n_phone_and_laptops, na.rm=TRUE)),
        keyby=.(date)] %>%
    tibbletime::tbl_time(index = date)

# Overall ratio of 'good' customers (as a numeric, not DF)
## This can be filtered to more recent dates etc.
is_good_customer_ratio <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted) %>%
  summarise(Count = sum(item_quantity)) %>%
  mutate(freq = Count / sum(Count)) %>%
  filter(is_fraud_blacklisted %in% 'False') %>%
  pull(freq)

###
### Remainder score
### `mean_sum_committed_sub_value_day`
###
mean_sum_committed_sub_value_day <-
    select(tibbletime_metrics_means, date, mean_sum_committed_sub_value) %>%
    time_decompose(mean_sum_committed_sub_value, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_sum_committed_sub_value_day <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, sum_committed_sub_value, is_fraud_blacklisted) %>%
    left_join(mean_sum_committed_sub_value_day[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_sum_committed_sub_value_day = sum_committed_sub_value - season - trend,
           z_remainder_mean_sum_committed_sub_value_day =
             as.numeric(scale(remainder_mean_sum_committed_sub_value_day)))
rm(mean_sum_committed_sub_value_day)

###
### Remainder score
### `amount_orders`
###
mean_amount_orders_day <-
    select(tibbletime_metrics_means, date, mean_amount_orders_day) %>%
    time_decompose(mean_amount_orders_day, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_orders <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, number_orders, is_fraud_blacklisted) %>%
    left_join(mean_amount_orders_day[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_amount_orders_day = number_orders - season - trend,
           z_remainder_amount_orders_day = as.numeric(scale(remainder_amount_orders_day)))
rm(mean_amount_orders_day)

###
### Remainder score
### `item_quantity`
###
mean_sum_item_quantity <-
    select(tibbletime_metrics_means, date, mean_sum_item_quantity) %>%
    time_decompose(mean_sum_item_quantity, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_sum_item_quantity <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, sum_item_quantity, is_fraud_blacklisted) %>%
    left_join(mean_sum_item_quantity[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_sum_item_quantity = sum_item_quantity - season - trend,
           z_remainder_sum_item_quantity = as.numeric(scale(remainder_sum_item_quantity)))
rm(mean_sum_item_quantity)

###
### Remainder score
### `amount_duplicate_payment_methods`
###
mean_amount_duplicate_pm <-
    select(tibbletime_metrics_means, date, mean_duplicate_pm) %>%
    time_decompose(mean_duplicate_pm, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_duplicate_pm <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, duplicate_payment_methods, is_fraud_blacklisted) %>%
    left_join(mean_amount_duplicate_pm[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_duplicate_pm = duplicate_payment_methods - season - trend,
           z_remainder_mean_duplicate_pm = as.numeric(scale(remainder_mean_duplicate_pm)))
rm(mean_amount_duplicate_pm)

###
### Remainder score
### `amount_subscriptions_above_12`
###
mean_multiple_12 <-
    select(tibbletime_metrics_means, date, mean_multiple_12) %>%
    time_decompose(mean_multiple_12, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_multiple_12 <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_subs_above_12, is_fraud_blacklisted) %>%
    left_join(mean_multiple_12[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_amount_multiple_12 = n_subs_above_12 - season - trend,
           z_remainder_mean_amount_multiple_12 = as.numeric(scale(remainder_mean_amount_multiple_12)))
rm(mean_multiple_12)

###
### Remainder score
### `amount_multiple_phones_laptops`
###
mean_amount_multiple_phones_laptops <-
    select(tibbletime_metrics_means, date, mean_multiple_pl) %>%
    time_decompose(mean_multiple_pl, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_multiple_phones_laptops <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_phone_and_laptops, is_fraud_blacklisted) %>%
    left_join(mean_amount_multiple_phones_laptops[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_multiple_pl = n_phone_and_laptops - season - trend,
           z_remainder_mean_multiple_pl = as.numeric(scale(remainder_mean_multiple_pl)))
rm(mean_amount_multiple_phones_laptops)
rm(tibbletime_metrics_means)

###
### `remainder_scores` - Join all remainder score DFs
###
remainder_scores <-
    ts_mean_sum_committed_sub_value_day %>%
    select(customer_id, date, z_remainder_mean_sum_committed_sub_value_day, is_fraud_blacklisted)

## ts_amount_orders
remainder_scores <-
  ts_amount_orders %>%
  select(customer_id, date, z_remainder_amount_orders_day) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_sum_item_quantity
remainder_scores <-
  ts_sum_item_quantity %>%
  select(customer_id, date, z_remainder_sum_item_quantity) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_duplicate_payment_methods
remainder_scores <-
  ts_amount_duplicate_pm %>%
  select(customer_id, date, z_remainder_mean_duplicate_pm) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_multiple_phones_laptops
remainder_scores <-
  ts_amount_multiple_phones_laptops %>%
  select(customer_id, date, z_remainder_mean_multiple_pl) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_mean_multiple_12
remainder_scores <-
  ts_mean_multiple_12 %>%
  select(customer_id, date, z_remainder_mean_amount_multiple_12) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

###
### Remainder score
### Create `remainder_score`
###
remainder_scores <-
  remainder_scores %>%
  mutate(combined_remainder_score =
           1 * z_remainder_mean_sum_committed_sub_value_day +
           1 * z_remainder_amount_orders_day +
           1 * z_remainder_sum_item_quantity +
           1 * z_remainder_mean_duplicate_pm +
           1 * z_remainder_mean_multiple_pl +
           1 * z_remainder_mean_amount_multiple_12) %>%
  mutate(z_remainder_score = as.numeric(scale(combined_remainder_score)))
rm(list = ls()[grepl(pattern = '^ts_', x = ls(), ignore.case = TRUE)])

###
### Calculate Penalty scores
###

###
### Penalty score
### `Time/hour of order`
###

# How many subscriptions were made at each hour
## This can be filtered to more recent dates etc.
hour_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(time) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each hour which tells us something about the likelihood of
# 'not good' customers during that hour
overall_time_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, time) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(time, names_from = is_fraud_blacklisted, values_from = Count) %>%
  left_join(hour_count, by = 'time') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(time, contains('_ratio')) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(time, False_ratio) %>%
  rename(time_penalty = "False_ratio")

z_score_mean_time_penalty <-
    data.table(left_join(subscriptions, overall_time_penalty, by = 'time'))[
    ,.(customer_id = first_DT(customer_id),
       time_penalty = first_DT(time_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_time_penalty = mean(time_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_time_penalty = as.numeric(scale(mean_time_penalty))) %>%
    tibble()
rm(hour_count, overall_time_penalty)

###
### Penalty score
### `email_domain`
###
email_domain_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
  group_by(domain) %>%
  summarise(Count = sum(item_quantity))

overall_domain_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
  group_by(is_fraud_blacklisted, domain) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(domain, names_from = is_fraud_blacklisted, values_from = Count) %>%
  left_join(email_domain_count, by = 'domain') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(domain, contains('_ratio'), Count) %>%
  arrange(desc(Count)) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(domain, False_ratio) %>%
  rename(domain_penalty = "False_ratio")

domain_group_penalty_domain_level <-
  overall_domain_penalty %>%
  mutate(domain_group = NA) %>%
  mutate(domain_group = if_else(round(domain_penalty, 2) <= 0.831, 0, NA_real_)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.001, 1, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.101, 2, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.751, 3, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) > 1.751, 4, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group), 99, domain_group))

overall_domain_group_penalty <-
  domain_group_penalty_domain_level %>%
  group_by(domain_group) %>%
  summarise(domain_group_penalty = mean(domain_penalty)) %>%
  mutate(domain_group_penalty = if_else(is.na(domain_group_penalty),
                                        median(domain_group_penalty_domain_level$domain_penalty, na.rm = TRUE),
                                        domain_group_penalty))

overall_domain_group_penalty <-
  domain_group_penalty_domain_level %>%
  left_join(overall_domain_group_penalty, by = 'domain_group')

subscriptions_domain_group_penalty <-
    subscriptions %>%
    select(customer_id, order_id, date, email) %>%
    distinct() %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    left_join(overall_domain_group_penalty, by = 'domain')

z_score_mean_domain_group_penalty <-
    data.table(subscriptions_domain_group_penalty)[
    ,.(customer_id = first_DT(customer_id),
       domain_group_penalty = first_DT(domain_group_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_domain_group_penalty = mean(domain_group_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_domain_group_penalty = as.numeric(scale(mean_domain_group_penalty))) %>%
    mutate(z_score_mean_domain_group_penalty = replace_na(z_score_mean_domain_group_penalty, 0))

rm(subscriptions_domain_group_penalty)
rm(email_domain_count, overall_domain_penalty, overall_domain_group_penalty,
   domain_group_penalty_domain_level)

###
### Penalty score
### Postal code (full or 2-digits) penalty
###
# Set thresholds to use full postal code
use_full_pc_threshold_items <- 50
use_full_pc_threshold_customers <- 5

# Counts for the full postal code (filter out below thresholds)
pc_count <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 5) %>%
    group_by(shipping_pc) %>%
    summarise(Count = sum(item_quantity),
              d_customers = n_distinct(customer_id)) %>%
    filter(Count >= use_full_pc_threshold_items,
           d_customers >= use_full_pc_threshold_customers)

# Counts for the pc2 code (not in `pc_count` already)
pc2_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False'),
         nchar(shipping_pc) %in% 5,
         !shipping_pc %in% pc_count$shipping_pc) %>%
  mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
  group_by(pc2) %>%
  summarise(Count = sum(item_quantity))

# Combine pc and pc2 counts
pc_count <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 5) %>%
    select(shipping_pc) %>%
    distinct() %>%
    mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
    left_join(pc_count, by = 'shipping_pc') %>%
    left_join(pc2_count, by = 'pc2', suffix = c('.pc5', '.pc2')) %>%
    mutate(pc = if_else(!is.na(Count.pc5), shipping_pc, pc2),
           Count = if_else(!is.na(Count.pc5), Count.pc5, Count.pc2)) %>%
    select(pc, Count) %>%
    distinct()
rm(pc2_count)

# Calculate a 'penalty' for each pc which tells us something about the likelihood of
# 'not good' customers in that pc
overall_pc_penalty <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 5) %>%
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, substr(shipping_pc, 1, 2))) %>%
    group_by(is_fraud_blacklisted, pc) %>%
    summarise(Count = sum(item_quantity)) %>%
    pivot_wider(pc, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(pc_count, by = 'pc') %>%
    mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
    select(pc, contains('_ratio')) %>%
    mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
    select(pc, False_ratio) %>%
    rename(pc_penalty = "False_ratio") %>%
    mutate(pc_penalty = if_else(!is.finite(pc_penalty), min(pc_penalty), pc_penalty))

# Create DF with the max pc penalty (and z-score) for each combination of
# customer_id and date
z_score_max_pc_penalty <-
    subscriptions %>%
    filter(nchar(shipping_pc) %in% 5) %>%
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, substr(shipping_pc, 1, 2))) %>%
    left_join(overall_pc_penalty, by = 'pc') %>%
    group_by(customer_id, date) %>%
    summarise(max_pc_penalty = max(pc_penalty)) %>%
    ungroup() %>%
    mutate(z_score_max_pc_penalty = as.numeric(scale(max_pc_penalty)))

# Add all customer_id & date combinations of 'subscriptions'
# Some shipping_pc might be new or don't have labels yet
z_score_max_pc_penalty <-
    subscriptions %>%
    filter(nchar(shipping_pc) %in% 5) %>%
    left_join(z_score_max_pc_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_max_pc_penalty = replace_na(z_score_max_pc_penalty, 0)) %>%
    select(customer_id, date, z_score_max_pc_penalty) %>%
    distinct()
rm(pc_count, overall_pc_penalty)

###
### Penalty score
### Asset risk category penalty
###
z_score_max_asset_risk_penalty <-
  subscriptions %>%
  ungroup() %>%
  group_by(customer_id, date) %>%
  summarise(max_asset_risk_category_fraud_rate = max(asset_risk_category_fraud_rate)) %>%
  ungroup() %>%
  mutate(z_max_asset_category_penalty = as.numeric(scale(max_asset_risk_category_fraud_rate)))

###
### Penalty score
### `multiple_user_id`
###

# Add 'unique_user_id2' to 'subscriptions' via left join
# Set NA in unique_user_id2 to 0 (zero multiple user_id)
# Round values in 'unique_user_id2':
#  - to 5 over 10
#  - to 10 over 50
#  - to 100 over 100
subscriptions <-
  subscriptions %>%
  left_join(total_logins_dates[, c('customer_id', 'day', 'unique_user_id2')],
            by = c('customer_id', 'day')) %>%
  mutate(unique_user_id2 = replace_na(unique_user_id2, 0),
         unique_user_id2 = as.numeric(unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 5,
                                   round_any(unique_user_id2, 5), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 10,
                                   round_any(unique_user_id2, 25), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 50,
                                   round_any(unique_user_id2, 50), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 100, 100, unique_user_id2))

# How many subscriptions were made for each (rounded) unique multiple user ID amount
## This can be filtered to more recent dates etc.
multiple_user_id_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(unique_user_id2) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each unique multiple user ID amount which tells us something
# about the likelihood of 'not good' customers for that amount
overall_multiple_user_id_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, unique_user_id2) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(id_cols = unique_user_id2, names_from = is_fraud_blacklisted,
              values_from = Count, values_fill = 0) %>%
  left_join(multiple_user_id_count, by = 'unique_user_id2') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(unique_user_id2, contains('_ratio')) %>%
  mutate(unique_user_id2 = as.character(unique_user_id2)) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(unique_user_id2, False_ratio) %>%
  rename(multiple_user_id_penalty = "False_ratio") %>%
  mutate(multiple_user_id_penalty = if_else(!is.finite(multiple_user_id_penalty),
                                            min(multiple_user_id_penalty),
                                            multiple_user_id_penalty))

z_score_mean_multiple_user_id_penalty <-
    data.table(left_join(mutate(subscriptions, unique_user_id2 = as.character(unique_user_id2)),
                         # to character for proper treatment (easier join)
                         overall_multiple_user_id_penalty, by = 'unique_user_id2'))[
    ,.(customer_id = first_DT(customer_id),
       multiple_user_id_penalty = first_DT(multiple_user_id_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_multiple_user_id_penalty = mean(multiple_user_id_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_multiple_user_id_penalty = as.numeric(scale(mean_multiple_user_id_penalty))) %>%
    tibble()
rm(total_logins_dates, multiple_user_id_count, overall_multiple_user_id_penalty)

###
### Penalty score
### `payment_category_penalty`
###

# Calculate z-score based on max 'is_fraud_blacklisted_ratio' per user & date
z_payment_category_penalty <-
    subscriptions %>%
    ungroup() %>%
    group_by(customer_id, date) %>%
    summarise(is_fraud_blacklisted_ratio = max(is_fraud_blacklisted_ratio),
              is_fraud_blacklisted = first(is_fraud_blacklisted),
              item_quantity = sum(item_quantity)) %>%
    ungroup() %>%
    mutate(z_payment_category_penalty = as.numeric(scale(is_fraud_blacklisted_ratio))) %>%
    select(customer_id, date, z_payment_category_penalty)

###
### Penalty score
### `nethone_worst_signal`
###

nethone_worst_signal_count <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(nethone_worst_signal) %>%
    summarise(Count = n())

overall_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(is_fraud_blacklisted, nethone_worst_signal) %>%
    summarise(Count = n()) %>%
    pivot_wider(nethone_worst_signal, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(nethone_worst_signal_count, by = 'nethone_worst_signal') %>%
    mutate(False_ratio = False / Count,
           True_ratio = True / Count) %>%
    select(nethone_worst_signal, contains('_ratio')) %>%
    mutate(False_ratio = is_good_customer_ratio / False_ratio,
           True_ratio = is_good_customer_ratio / True_ratio) %>%
    select(nethone_worst_signal, False_ratio) %>%
    rename(nethone_worst_signal_penalty = "False_ratio")

z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal)) %>%
    left_join(overall_nethone_worst_signal_penalty, by = 'nethone_worst_signal') %>%
    group_by(customer_id, date) %>%
    summarise(nethone_worst_signal_penalty = max(nethone_worst_signal_penalty)) %>%
    ungroup() %>%
    mutate(z_score_nethone_worst_signal_penalty = as.numeric(scale(nethone_worst_signal_penalty))) %>%
    mutate(z_score_nethone_worst_signal_penalty = if_else(is.na(z_score_nethone_worst_signal_penalty),
                                                          0, z_score_nethone_worst_signal_penalty))

# Add all customer_id & date combinations of 'subscriptions'
z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    left_join(z_score_nethone_worst_signal_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_nethone_worst_signal_penalty = replace_na(z_score_nethone_worst_signal_penalty, 0)) %>%
    select(customer_id, date, z_score_nethone_worst_signal_penalty) %>%
    distinct()
rm(nethone_worst_signal_count, overall_nethone_worst_signal_penalty)

###
### Combine remainder and penalty scores
### `remainder_scores_penalties`
###
remainder_scores_penalties <-
    remainder_scores %>%
    left_join(z_score_max_pc_penalty[,c('customer_id','date','z_score_max_pc_penalty')],
              by=c('customer_id','date')) %>%
    left_join(z_score_mean_domain_group_penalty[,c('customer_id','date','z_score_mean_domain_group_penalty')],
              by=c('customer_id','date')) %>%
    left_join(z_score_mean_time_penalty[,c('customer_id','date','z_score_mean_time_penalty')],
              by=c('customer_id','date')) %>%
    left_join(select(z_score_mean_multiple_user_id_penalty, customer_id, date,
                     z_score_mean_multiple_user_id_penalty),
                     by = c('customer_id', 'date')) %>%
    left_join(select(z_payment_category_penalty, customer_id, date, z_payment_category_penalty),
                     by = c('customer_id', 'date')) %>%
    left_join(select(z_score_max_asset_risk_penalty, customer_id, date, z_max_asset_category_penalty),
                     by = c('customer_id', 'date')) %>%
    left_join(select(z_score_nethone_worst_signal_penalty, customer_id, date, z_score_nethone_worst_signal_penalty),
                     by = c('customer_id', 'date'))
rm(list = ls()[grepl(pattern = '^z_score_mean', x = ls())])

###
### Create anomaly score
###
anomaly_score <-
  subscriptions %>%
  select(customer_id, date) %>%
  right_join(remainder_scores_penalties, by=c('customer_id','date'))
rm(remainder_scores_penalties)

###
### Apply optimized parameters to calculate anomaly score
###
params <- c(0.154, 0.300, 0.000, 0.232, 0.056, 0.808, 0.479, 1.002, 1.647, 2.152, 0.633, 1.483, 0.000)
params <- c(1.635, 0.492, 0.085, 0.942, 0.241, 3.680, 2.733, 6.293, 10.00, 9.987, 3.462, 8.378, 7.981)

###
### Create optim_scores (optimized anomaly_score)
###
optim_scores <-
  anomaly_score %>%
    mutate(anomaly_score =
           params[1] * z_remainder_mean_sum_committed_sub_value_day +
           params[2] * z_remainder_amount_orders_day +
           params[3] * z_remainder_sum_item_quantity +
           params[4] * z_remainder_mean_duplicate_pm +
           params[5] * z_remainder_mean_multiple_pl +
           params[6] * z_remainder_mean_amount_multiple_12 +
           params[7] * z_score_mean_time_penalty +
           params[8] * z_score_mean_domain_group_penalty +
           params[9] * z_score_max_pc_penalty +
           params[10] * z_max_asset_category_penalty +
           params[11] * z_score_mean_multiple_user_id_penalty +
           params[12] * z_payment_category_penalty +
           params[13] * z_score_nethone_worst_signal_penalty) %>%
  mutate(anomaly_score = as.numeric(scale(anomaly_score)))

# Reduce anomaly_score for good labels from 'hist_labels', re-calculate anomaly_score
optim_scores <-
    optim_scores %>%
    left_join(hist_labels, by = c('customer_id', c('date' = 'day'))) %>%
    mutate(anomaly_score = if_else(label_state %in% c('good'), anomaly_score - 2.5, anomaly_score),
           anomaly_score = if_else(label_state %in% c('uncertain'), anomaly_score - 2, anomaly_score),
           anomaly_score = as.numeric(scale(anomaly_score)))

###
### Create 'anomaly_score_daily' and save to .csv file
###
anomaly_score_daily <-
  optim_scores %>%
  rownames_to_column('id') %>%
  mutate(created_at = now()) %>%
  rename(score_day = date) %>%
  select(created_at, customer_id, score_day, anomaly_score,
         z_remainder_mean_amount_multiple_12, z_remainder_mean_multiple_pl,
         z_remainder_mean_duplicate_pm, z_remainder_mean_sum_committed_sub_value_day,
         z_score_max_pc_penalty, z_score_mean_domain_group_penalty, z_score_mean_time_penalty,
         z_remainder_sum_item_quantity, z_max_asset_category_penalty,
         z_remainder_amount_orders_day, z_score_mean_multiple_user_id_penalty,
         z_payment_category_penalty, z_score_nethone_worst_signal_penalty)

# Create anomaly scores on 'order_id' level for the last few days
anomaly_score_order <-
  subscriptions %>%
  select(customer_id, order_id, date) %>%
  filter(date >= today() - 8) %>%
  distinct() %>%
  left_join(select(anomaly_score_daily, customer_id, score_day, anomaly_score),
            by = c('customer_id', 'date' = 'score_day')) %>%
  mutate(anomaly_score = if_else(customer_id %in% recent_customers_onfido_verified,
                                 anomaly_score - 0.5, anomaly_score)) %>%
  select(order_id, anomaly_score) %>%
  mutate(created_at = now())

## Write to `anomaly_score_order_germany.csv`
write.csv(anomaly_score_order, 'anomaly_score_order_germany.csv')

################
### Austria ####
################

###
### Preperation
###
# Fresh start / clean workspace
rm(list = ls())

###
### Load data from preparation script and adjust for Austria
###
load('prep_data_all_countries.RData')

# Only keep data from Austria
subscriptions <-
    subscriptions %>%
    filter(shipping_country %in% 'Austria',
           date >= start_date) %>%
    select(-shipping_country)

# Only keep data from Austria
total_logins_dates <-
    total_logins_dates %>%
    filter(customer_id %in% subscriptions$customer_id,
           day >= start_date)

# Only keep data from Austria; change grouping to 'customer_id' & 'date'
customer_level_ts <-
    customer_level_ts %>%
    ungroup() %>%
    filter(shipping_country %in% 'Austria',
           date >= start_date) %>%
    select(-shipping_country) %>%
    group_by(customer_id, date)

###
### Calculate all metrics for anomaly_score (Austria)
###

# Calculate the means of the metrics used
tibbletime_metrics_means <-
    data.table(customer_level_ts)[date >= start_date & date <= end_date][
        , .(mean_sum_committed_sub_value = mean(sum_committed_sub_value, na.rm=TRUE),
            mean_amount_orders_day = mean(number_orders, na.rm=TRUE),
            mean_sum_item_quantity = mean(sum_item_quantity, na.rm=TRUE),
            mean_duplicate_pm = mean(duplicate_payment_methods, na.rm=TRUE),
            mean_multiple_12 = mean(n_subs_above_12, na.rm=TRUE),
            mean_multiple_pl = mean(n_phone_and_laptops, na.rm=TRUE)),
        keyby=.(date)] %>%
    tibbletime::tbl_time(index = date)

# Overall ratio of 'good' customers (as a numeric, not DF)
## This can be filtered to more recent dates etc.
is_good_customer_ratio <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted) %>%
  summarise(Count = sum(item_quantity)) %>%
  mutate(freq = Count / sum(Count)) %>%
  filter(is_fraud_blacklisted %in% 'False') %>%
  pull(freq)

###
### Remainder score
### `mean_sum_committed_sub_value_day`
###
mean_sum_committed_sub_value_day <-
    select(tibbletime_metrics_means, date, mean_sum_committed_sub_value) %>%
    time_decompose(mean_sum_committed_sub_value, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_sum_committed_sub_value_day <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, sum_committed_sub_value, is_fraud_blacklisted) %>%
    left_join(mean_sum_committed_sub_value_day[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_sum_committed_sub_value_day = sum_committed_sub_value - season - trend,
           z_remainder_mean_sum_committed_sub_value_day =
             as.numeric(scale(remainder_mean_sum_committed_sub_value_day)))
rm(mean_sum_committed_sub_value_day)

###
### Remainder score
### `amount_orders`
###
mean_amount_orders_day <-
    select(tibbletime_metrics_means, date, mean_amount_orders_day) %>%
    time_decompose(mean_amount_orders_day, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_orders <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, number_orders, is_fraud_blacklisted) %>%
    left_join(mean_amount_orders_day[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_amount_orders_day = number_orders - season - trend,
           z_remainder_amount_orders_day = as.numeric(scale(remainder_amount_orders_day)))
rm(mean_amount_orders_day)

###
### Remainder score
### `item_quantity`
###
mean_sum_item_quantity <-
    select(tibbletime_metrics_means, date, mean_sum_item_quantity) %>%
    time_decompose(mean_sum_item_quantity, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_sum_item_quantity <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, sum_item_quantity, is_fraud_blacklisted) %>%
    left_join(mean_sum_item_quantity[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_sum_item_quantity = sum_item_quantity - season - trend,
           z_remainder_sum_item_quantity = as.numeric(scale(remainder_sum_item_quantity)))
rm(mean_sum_item_quantity)

###
### Remainder score
### `amount_duplicate_payment_methods`
###
mean_amount_duplicate_pm <-
    select(tibbletime_metrics_means, date, mean_duplicate_pm) %>%
    time_decompose(mean_duplicate_pm, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_duplicate_pm <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, duplicate_payment_methods, is_fraud_blacklisted) %>%
    left_join(mean_amount_duplicate_pm[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_duplicate_pm = duplicate_payment_methods - season - trend,
           z_remainder_mean_duplicate_pm = as.numeric(scale(remainder_mean_duplicate_pm)))
rm(mean_amount_duplicate_pm)

###
### Remainder score
### `amount_subscriptions_above_12`
###
mean_multiple_12 <-
    select(tibbletime_metrics_means, date, mean_multiple_12) %>%
    time_decompose(mean_multiple_12, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_multiple_12 <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_subs_above_12, is_fraud_blacklisted) %>%
    left_join(mean_multiple_12[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_amount_multiple_12 = n_subs_above_12 - season - trend,
           z_remainder_mean_amount_multiple_12 = as.numeric(scale(remainder_mean_amount_multiple_12)))
rm(mean_multiple_12)

###
### Remainder score
### `amount_multiple_phones_laptops`
###
mean_amount_multiple_phones_laptops <-
    select(tibbletime_metrics_means, date, mean_multiple_pl) %>%
    time_decompose(mean_multiple_pl, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_multiple_phones_laptops <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_phone_and_laptops, is_fraud_blacklisted) %>%
    left_join(mean_amount_multiple_phones_laptops[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_multiple_pl = n_phone_and_laptops - season - trend,
           z_remainder_mean_multiple_pl = as.numeric(scale(remainder_mean_multiple_pl)))
rm(mean_amount_multiple_phones_laptops)
rm(tibbletime_metrics_means)

###
### `remainder_scores` - Join all remainder score DFs
###
remainder_scores <-
    ts_mean_sum_committed_sub_value_day %>%
    select(customer_id, date, z_remainder_mean_sum_committed_sub_value_day, is_fraud_blacklisted)

## ts_amount_orders
remainder_scores <-
  ts_amount_orders %>%
  select(customer_id, date, z_remainder_amount_orders_day) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_sum_item_quantity
remainder_scores <-
  ts_sum_item_quantity %>%
  select(customer_id, date, z_remainder_sum_item_quantity) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_duplicate_payment_methods
remainder_scores <-
  ts_amount_duplicate_pm %>%
  select(customer_id, date, z_remainder_mean_duplicate_pm) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_multiple_phones_laptops
remainder_scores <-
  ts_amount_multiple_phones_laptops %>%
  select(customer_id, date, z_remainder_mean_multiple_pl) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_mean_multiple_12
remainder_scores <-
  ts_mean_multiple_12 %>%
  select(customer_id, date, z_remainder_mean_amount_multiple_12) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

###
### Remainder score
### Create `remainder_score`
###
remainder_scores <-
  remainder_scores %>%
  mutate(combined_remainder_score =
           1 * z_remainder_mean_sum_committed_sub_value_day +
           1 * z_remainder_amount_orders_day +
           1 * z_remainder_sum_item_quantity +
           1 * z_remainder_mean_duplicate_pm +
           1 * z_remainder_mean_multiple_pl +
           1 * z_remainder_mean_amount_multiple_12) %>%
  mutate(z_remainder_score = as.numeric(scale(combined_remainder_score)))
rm(list = ls()[grepl(pattern = '^ts_', x = ls(), ignore.case = TRUE)])

###
### Calculate Penalty scores
###

###
### Penalty score
### `Time/hour of order`
###

# How many subscriptions were made at each hour
## This can be filtered to more recent dates etc.
hour_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(time) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each hour which tells us something about the likelihood of
# 'not good' customers during that hour
overall_time_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, time) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(time, names_from = is_fraud_blacklisted, values_from = Count) %>%
  left_join(hour_count, by = 'time') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(time, contains('_ratio')) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(time, False_ratio) %>%
  rename(time_penalty = "False_ratio")

z_score_mean_time_penalty <-
    data.table(left_join(subscriptions, overall_time_penalty, by = 'time'))[
    ,.(customer_id = first_DT(customer_id),
       time_penalty = first_DT(time_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_time_penalty = mean(time_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_time_penalty = as.numeric(scale(mean_time_penalty))) %>%
    tibble()
rm(hour_count, overall_time_penalty)

###
### Penalty score
### `email_domain`
###
email_domain_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
  group_by(domain) %>%
  summarise(Count = sum(item_quantity))

overall_domain_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
  group_by(is_fraud_blacklisted, domain) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(domain, names_from = is_fraud_blacklisted, values_from = Count) %>%
  left_join(email_domain_count, by = 'domain') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(domain, contains('_ratio'), Count) %>%
  arrange(desc(Count)) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(domain, False_ratio) %>%
  rename(domain_penalty = "False_ratio")

domain_group_penalty_domain_level <-
  overall_domain_penalty %>%
  mutate(domain_group = NA) %>%
  mutate(domain_group = if_else(round(domain_penalty, 2) <= 0.831, 0, NA_real_)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.001, 1, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.101, 2, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.751, 3, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) > 1.751, 4, domain_group)) %>%
  mutate(domain_group = if_else(is.na(domain_group), 99, domain_group))

overall_domain_group_penalty <-
  domain_group_penalty_domain_level %>%
  group_by(domain_group) %>%
  summarise(domain_group_penalty = mean(domain_penalty)) %>%
  mutate(domain_group_penalty = if_else(is.na(domain_group_penalty),
                                        median(domain_group_penalty_domain_level$domain_penalty, na.rm = TRUE),
                                        domain_group_penalty))

overall_domain_group_penalty <-
  domain_group_penalty_domain_level %>%
  left_join(overall_domain_group_penalty, by = 'domain_group')

subscriptions_domain_group_penalty <-
    subscriptions %>%
    select(customer_id, order_id, date, email) %>%
    distinct() %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    left_join(overall_domain_group_penalty, by = 'domain')

z_score_mean_domain_group_penalty <-
    data.table(subscriptions_domain_group_penalty)[
    ,.(customer_id = first_DT(customer_id),
       domain_group_penalty = first_DT(domain_group_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_domain_group_penalty = mean(domain_group_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_domain_group_penalty = as.numeric(scale(mean_domain_group_penalty))) %>%
    mutate(z_score_mean_domain_group_penalty = replace_na(z_score_mean_domain_group_penalty, 0))

rm(subscriptions_domain_group_penalty)
rm(email_domain_count, overall_domain_penalty, overall_domain_group_penalty,
   domain_group_penalty_domain_level)

###
### Penalty score
### Postal code (full or 2-digits) penalty
###
###
### Penalty score
### Postal code (full or 2-digits) penalty
###
# Set thresholds to use full postal code
use_full_pc_threshold_items <- 25
use_full_pc_threshold_customers <- 3

# Counts for the full postal code (filter out below thresholds)
pc_count <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 4) %>%
    group_by(shipping_pc) %>%
    summarise(Count = sum(item_quantity),
              d_customers = n_distinct(customer_id)) %>%
    filter(Count >= use_full_pc_threshold_items,
           d_customers >= use_full_pc_threshold_customers)

# Counts for the pc2 code (not in `pc_count` already)
pc2_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False'),
         nchar(shipping_pc) %in% 4,
         !shipping_pc %in% pc_count$shipping_pc) %>%
  mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
  group_by(pc2) %>%
  summarise(Count = sum(item_quantity))

# Combine pc and pc2 counts
pc_count <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 4) %>%
    select(shipping_pc) %>%
    distinct() %>%
    mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
    left_join(pc_count, by = 'shipping_pc') %>%
    left_join(pc2_count, by = 'pc2', suffix = c('.pc5', '.pc2')) %>%
    mutate(pc = if_else(!is.na(Count.pc5), shipping_pc, pc2),
           Count = if_else(!is.na(Count.pc5), Count.pc5, Count.pc2)) %>%
    select(pc, Count) %>%
    distinct()
rm(pc2_count)

# Calculate a 'penalty' for each pc which tells us something about the likelihood of
# 'not good' customers in that pc
overall_pc_penalty <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 4) %>%
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, substr(shipping_pc, 1, 2))) %>%
    group_by(is_fraud_blacklisted, pc) %>%
    summarise(Count = sum(item_quantity)) %>%
    pivot_wider(pc, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(pc_count, by = 'pc') %>%
    mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
    select(pc, contains('_ratio')) %>%
    mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
    select(pc, False_ratio) %>%
    rename(pc_penalty = "False_ratio") %>%
    mutate(pc_penalty = if_else(!is.finite(pc_penalty),
                                min(pc_penalty),
                                pc_penalty))

# Create DF with the max pc penalty (and z-score) for each combination of
# customer_id and date
z_score_max_pc_penalty <-
    subscriptions %>%
    filter(nchar(shipping_pc) %in% 4) %>%
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, substr(shipping_pc, 1, 2))) %>%
    left_join(overall_pc_penalty, by = 'pc') %>%
    group_by(customer_id, date) %>%
    summarise(max_pc_penalty = max(pc_penalty)) %>%
    ungroup() %>%
    mutate(z_score_max_pc_penalty = as.numeric(scale(max_pc_penalty)))

# Add all customer_id & date combinations of 'subscriptions'
# Some shipping_pc might be new or don't have labels yet
z_score_max_pc_penalty <-
    subscriptions %>%
    filter(nchar(shipping_pc) %in% 4) %>%
    left_join(z_score_max_pc_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_max_pc_penalty = replace_na(z_score_max_pc_penalty, 0)) %>%
    select(customer_id, date, z_score_max_pc_penalty) %>%
    distinct()
rm(pc_count, overall_pc_penalty)

###
### Penalty score
### Asset risk category penalty
###
z_score_max_asset_risk_penalty <-
  subscriptions %>%
  ungroup() %>%
  group_by(customer_id, date) %>%
  summarise(max_asset_risk_category_fraud_rate = max(asset_risk_category_fraud_rate)) %>%
  ungroup() %>%
  mutate(z_max_asset_category_penalty = as.numeric(scale(max_asset_risk_category_fraud_rate)))

###
### Penalty score
### `multiple_user_id`
###

# Add 'unique_user_id2' to 'subscriptions' via left join
# Set NA in unique_user_id2 to 0 (zero multiple user_id)
# Round values in 'unique_user_id2':
#  - to 5 over 10
#  - to 10 over 50
#  - to 100 over 100
subscriptions <-
  subscriptions %>%
  left_join(total_logins_dates[, c('customer_id', 'day', 'unique_user_id2')],
            by = c('customer_id', 'day')) %>%
  mutate(unique_user_id2 = replace_na(unique_user_id2, 0),
         unique_user_id2 = as.numeric(unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 5,
                                   round_any(unique_user_id2, 5), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 10,
                                   round_any(unique_user_id2, 25), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 50,
                                   round_any(unique_user_id2, 50), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 100, 100, unique_user_id2))

# How many subscriptions were made for each (rounded) unique multiple user ID amount
## This can be filtered to more recent dates etc.
multiple_user_id_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(unique_user_id2) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each unique multiple user ID amount which tells us something
# about the likelihood of 'not good' customers for that amount
overall_multiple_user_id_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, unique_user_id2) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(id_cols = unique_user_id2, names_from = is_fraud_blacklisted,
              values_from = Count, values_fill = 0) %>%
  left_join(multiple_user_id_count, by = 'unique_user_id2') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(unique_user_id2, contains('_ratio')) %>%
  mutate(unique_user_id2 = as.character(unique_user_id2)) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(unique_user_id2, False_ratio) %>%
  rename(multiple_user_id_penalty = "False_ratio") %>%
  mutate(multiple_user_id_penalty = if_else(!is.finite(multiple_user_id_penalty),
                                            min(multiple_user_id_penalty),
                                            multiple_user_id_penalty))

z_score_mean_multiple_user_id_penalty <-
    data.table(left_join(mutate(subscriptions, unique_user_id2 = as.character(unique_user_id2)),
                         # to character for proper treatment (easier join)
                         overall_multiple_user_id_penalty, by = 'unique_user_id2'))[
    ,.(customer_id = first_DT(customer_id),
       multiple_user_id_penalty = first_DT(multiple_user_id_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_multiple_user_id_penalty = mean(multiple_user_id_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_multiple_user_id_penalty = as.numeric(scale(mean_multiple_user_id_penalty))) %>%
    tibble()
rm(total_logins_dates, multiple_user_id_count, overall_multiple_user_id_penalty)

###
### Penalty score
### `payment_category_penalty`
###

# Calculate z-score based on max 'is_fraud_blacklisted_ratio' per user & date
z_payment_category_penalty <-
    subscriptions %>%
    ungroup() %>%
    group_by(customer_id, date) %>%
    summarise(is_fraud_blacklisted_ratio = max(is_fraud_blacklisted_ratio),
              is_fraud_blacklisted = first(is_fraud_blacklisted),
              item_quantity = sum(item_quantity)) %>%
    ungroup() %>%
    mutate(z_payment_category_penalty = as.numeric(scale(is_fraud_blacklisted_ratio))) %>%
    select(customer_id, date, z_payment_category_penalty)

###
### Penalty score
### `nethone_worst_signal`
###

nethone_worst_signal_count <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(nethone_worst_signal) %>%
    summarise(Count = n())

overall_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(is_fraud_blacklisted, nethone_worst_signal) %>%
    summarise(Count = n()) %>%
    pivot_wider(nethone_worst_signal, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(nethone_worst_signal_count, by = 'nethone_worst_signal') %>%
    mutate(False_ratio = False / Count,
           True_ratio = True / Count) %>%
    select(nethone_worst_signal, contains('_ratio')) %>%
    mutate(False_ratio = is_good_customer_ratio / False_ratio,
           True_ratio = is_good_customer_ratio / True_ratio) %>%
    select(nethone_worst_signal, False_ratio) %>%
    rename(nethone_worst_signal_penalty = "False_ratio")

z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal)) %>%
    left_join(overall_nethone_worst_signal_penalty, by = 'nethone_worst_signal') %>%
    group_by(customer_id, date) %>%
    summarise(nethone_worst_signal_penalty = max(nethone_worst_signal_penalty)) %>%
    ungroup() %>%
    mutate(z_score_nethone_worst_signal_penalty = as.numeric(scale(nethone_worst_signal_penalty))) %>%
    mutate(z_score_nethone_worst_signal_penalty = if_else(is.na(z_score_nethone_worst_signal_penalty),
                                                          0, z_score_nethone_worst_signal_penalty))

# Add all customer_id & date combinations of 'subscriptions'
z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    left_join(z_score_nethone_worst_signal_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_nethone_worst_signal_penalty = replace_na(z_score_nethone_worst_signal_penalty, 0)) %>%
    select(customer_id, date, z_score_nethone_worst_signal_penalty) %>%
    distinct()
rm(nethone_worst_signal_count, overall_nethone_worst_signal_penalty)

###
### Combine remainder and penalty scores
### `remainder_scores_penalties`
###
remainder_scores_penalties <-
    remainder_scores %>%
    left_join(z_score_max_pc_penalty[,c('customer_id','date','z_score_max_pc_penalty')],
              by=c('customer_id','date')) %>%
    left_join(z_score_mean_domain_group_penalty[,c('customer_id','date','z_score_mean_domain_group_penalty')],
              by=c('customer_id','date')) %>%
    left_join(z_score_mean_time_penalty[,c('customer_id','date','z_score_mean_time_penalty')],
              by=c('customer_id','date')) %>%
    left_join(select(z_score_max_asset_risk_penalty, customer_id, date, z_max_asset_category_penalty),
              by = c('customer_id', 'date')) %>%
    left_join(select(z_score_mean_multiple_user_id_penalty, customer_id, date,
                     z_score_mean_multiple_user_id_penalty),
                     by = c('customer_id', 'date')) %>%
    left_join(select(z_payment_category_penalty, customer_id, date, z_payment_category_penalty),
                     by = c('customer_id', 'date')) %>%
    left_join(select(z_score_nethone_worst_signal_penalty, customer_id, date, z_score_nethone_worst_signal_penalty),
                     by = c('customer_id', 'date'))
rm(list = ls()[grepl(pattern = '^z_score_mean', x = ls())])

###
### Create anomaly score
###
anomaly_score <-
  subscriptions %>%
  select(customer_id, date) %>%
  right_join(remainder_scores_penalties, by=c('customer_id','date'))
rm(remainder_scores_penalties)

###
### Apply optimized parameters to calculate anomaly score
###
params <- c(0.865, 0.000, 0.000, 0.000, 0.334, 0.956, 0.745, 1.375, 2.236, 3.706, 1.574, 3.539, 1.215)
params <- c(0.942, 0.000, 0.000, 0.000, 0.368, 1.173, 0.910, 0.190, 3.871, 5.436, 2.994, 5.000, 3.077)

###
### Create optim_scores (optimized anomaly_score)
###
optim_scores <-
  anomaly_score %>%
    mutate(anomaly_score =
           params[1] * z_remainder_mean_sum_committed_sub_value_day +
           params[2] * z_remainder_amount_orders_day +
           params[3] * z_remainder_sum_item_quantity +
           params[4] * z_remainder_mean_duplicate_pm +
           params[5] * z_remainder_mean_multiple_pl +
           params[6] * z_remainder_mean_amount_multiple_12 +
           params[7] * z_score_mean_time_penalty +
           params[8] * z_score_mean_domain_group_penalty +
           params[9] * z_score_max_pc_penalty +
           params[10] * z_max_asset_category_penalty +
           params[11] * z_score_mean_multiple_user_id_penalty +
           params[12] * z_payment_category_penalty +
           params[13] * z_score_nethone_worst_signal_penalty) %>%
  mutate(anomaly_score = as.numeric(scale(anomaly_score)))

# Reduce anomaly_score for good labels from 'hist_labels', re-calculate anomaly_score
optim_scores <-
    optim_scores %>%
    left_join(hist_labels, by = c('customer_id', c('date' = 'day'))) %>%
    mutate(anomaly_score = if_else(label_state %in% c('good'), anomaly_score - 2.5, anomaly_score),
           anomaly_score = if_else(label_state %in% c('uncertain'), anomaly_score - 2, anomaly_score),
           anomaly_score = as.numeric(scale(anomaly_score)))

###
### Create 'anomaly_score_daily' and save to .csv file
###
anomaly_score_daily <-
  optim_scores %>%
  rownames_to_column('id') %>%
  mutate(created_at = now()) %>%
  rename(score_day = date) %>%
  select(created_at, customer_id, score_day, anomaly_score,
         z_remainder_mean_amount_multiple_12, z_remainder_mean_multiple_pl,
         z_remainder_mean_duplicate_pm, z_remainder_mean_sum_committed_sub_value_day,
         z_score_max_pc_penalty, z_score_mean_domain_group_penalty, z_score_mean_time_penalty,
         z_payment_category_penalty, z_max_asset_category_penalty,
         z_remainder_amount_orders_day, z_remainder_sum_item_quantity,
         z_score_mean_multiple_user_id_penalty, z_score_nethone_worst_signal_penalty)

# Create anomaly scores on 'order_id' level for the last few days
anomaly_score_order <-
  subscriptions %>%
  select(customer_id, order_id, date) %>%
  filter(date >= today() - 8) %>%
  distinct() %>%
  left_join(select(anomaly_score_daily, customer_id, score_day, anomaly_score),
            by = c('customer_id', 'date' = 'score_day')) %>%
  mutate(anomaly_score = if_else(customer_id %in% recent_customers_onfido_verified,
                                 anomaly_score - 0.5, anomaly_score)) %>%
  select(order_id, anomaly_score) %>%
  mutate(created_at = now())

## Write to `anomaly_score_order_austria.csv`
write.csv(anomaly_score_order, 'anomaly_score_order_austria.csv')


####################
### Netherlands ####
####################

###
### Preperation
###
# Fresh start / clean workspace
rm(list = ls())

###
### Load data from preparation script and adjust for Netherlands
###
load('prep_data_all_countries.RData')

# Only keep data from Netherlands
subscriptions <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           date >= start_date)

# Only keep data from Netherlands
total_logins_dates <-
    total_logins_dates %>%
    filter(customer_id %in% subscriptions$customer_id,
           day >= start_date)

# Only keep data from Netherlands; change grouping to 'customer_id' & 'date'
customer_level_ts <-
    customer_level_ts %>%
    filter(shipping_country %in% c('Netherlands'),
           date >= start_date) %>%
    group_by(customer_id, date)

###
### Calculate all metrics for anomaly_score (Netherlands)
###

# Calculate the means of the metrics used
tibbletime_metrics_means <-
    data.table(customer_level_ts)[date >= start_date & date <= end_date][
        , .(mean_sum_committed_sub_value = mean(sum_committed_sub_value, na.rm=TRUE),
            mean_amount_orders_day = mean(number_orders, na.rm=TRUE),
            mean_sum_item_quantity = mean(sum_item_quantity, na.rm=TRUE),
            mean_duplicate_pm = mean(duplicate_payment_methods, na.rm=TRUE),
            mean_multiple_12 = mean(n_subs_above_12, na.rm=TRUE),
            mean_multiple_pl = mean(n_phone_and_laptops, na.rm=TRUE)),
        keyby=.(date)] %>%
    tibbletime::tbl_time(index = date)

# Overall ratio of 'good' customers (as a numeric, not DF)
## This can be filtered to more recent dates etc.
is_good_customer_ratio <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted) %>%
  summarise(Count = sum(item_quantity)) %>%
  mutate(freq = Count / sum(Count)) %>%
  filter(is_fraud_blacklisted %in% 'False') %>%
  pull(freq)

###
### Remainder score
### `mean_sum_committed_sub_value_day`
###
mean_sum_committed_sub_value_day <-
    select(tibbletime_metrics_means, date, mean_sum_committed_sub_value) %>%
    time_decompose(mean_sum_committed_sub_value, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_sum_committed_sub_value_day <-
  customer_level_ts %>%
  ungroup() %>%
  select(customer_id, date, sum_committed_sub_value, is_fraud_blacklisted) %>%
  left_join(mean_sum_committed_sub_value_day[, c('date', 'season', 'trend')], by = 'date') %>%
  mutate(remainder_mean_sum_committed_sub_value_day = sum_committed_sub_value - season - trend,
         z_remainder_mean_sum_committed_sub_value_day =
           as.numeric(scale(remainder_mean_sum_committed_sub_value_day)))
rm(mean_sum_committed_sub_value_day)

###
### Remainder score
### `amount_orders`
###
mean_amount_orders_day <-
    select(tibbletime_metrics_means, date, mean_amount_orders_day) %>%
    time_decompose(mean_amount_orders_day, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_orders <-
  customer_level_ts %>%
  ungroup() %>%
  select(customer_id, date, number_orders, is_fraud_blacklisted) %>%
  left_join(mean_amount_orders_day[, c('date', 'season', 'trend')], by = 'date') %>%
  mutate(remainder_amount_orders_day = number_orders - season - trend,
         z_remainder_amount_orders_day = as.numeric(scale(remainder_amount_orders_day)))
rm(mean_amount_orders_day)

###
### Remainder score
### `item_quantity`
###
mean_sum_item_quantity <-
    select(tibbletime_metrics_means, date, mean_sum_item_quantity) %>%
    time_decompose(mean_sum_item_quantity, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_sum_item_quantity <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, sum_item_quantity, is_fraud_blacklisted) %>%
    left_join(mean_sum_item_quantity[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_sum_item_quantity = sum_item_quantity - season - trend,
           z_remainder_sum_item_quantity = as.numeric(scale(remainder_sum_item_quantity)))
rm(mean_sum_item_quantity)

###
### Remainder score
### `amount_duplicate_payment_methods`
###
mean_amount_duplicate_pm <-
    select(tibbletime_metrics_means, date, mean_duplicate_pm) %>%
    time_decompose(mean_duplicate_pm, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_duplicate_pm <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, duplicate_payment_methods, is_fraud_blacklisted) %>%
    left_join(mean_amount_duplicate_pm[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_duplicate_pm = duplicate_payment_methods - season - trend,
           z_remainder_mean_duplicate_pm = as.numeric(scale(remainder_mean_duplicate_pm)))
rm(mean_amount_duplicate_pm)

###
### Remainder score
### `amount_subscriptions_above_12`
###
mean_multiple_12 <-
    select(tibbletime_metrics_means, date, mean_multiple_12) %>%
    time_decompose(mean_multiple_12, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_multiple_12 <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_subs_above_12, is_fraud_blacklisted) %>%
    left_join(mean_multiple_12[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_amount_multiple_12 = n_subs_above_12 - season - trend,
           z_remainder_mean_amount_multiple_12 = as.numeric(scale(remainder_mean_amount_multiple_12)))
rm(mean_multiple_12)

###
### Remainder score
### `amount_multiple_phones_laptops`
###
mean_amount_multiple_phones_laptops <-
    select(tibbletime_metrics_means, date, mean_multiple_pl) %>%
    time_decompose(mean_multiple_pl, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_multiple_phones_laptops <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_phone_and_laptops, is_fraud_blacklisted) %>%
    left_join(mean_amount_multiple_phones_laptops[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_multiple_pl = n_phone_and_laptops - season - trend,
           z_remainder_mean_multiple_pl = as.numeric(scale(remainder_mean_multiple_pl)))
rm(mean_amount_multiple_phones_laptops)

###
### `remainder_scores` - Join all remainder score DFs
###
remainder_scores <-
    ts_mean_sum_committed_sub_value_day %>%
    select(customer_id, date, z_remainder_mean_sum_committed_sub_value_day, is_fraud_blacklisted)

## ts_amount_orders
remainder_scores <-
  ts_amount_orders %>%
  select(customer_id, date, z_remainder_amount_orders_day) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_sum_item_quantity
remainder_scores <-
  ts_sum_item_quantity %>%
  select(customer_id, date, z_remainder_sum_item_quantity) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_duplicate_payment_methods
remainder_scores <-
  ts_amount_duplicate_pm %>%
  select(customer_id, date, z_remainder_mean_duplicate_pm) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_multiple_phones_laptops
remainder_scores <-
  ts_amount_multiple_phones_laptops %>%
  select(customer_id, date, z_remainder_mean_multiple_pl) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_mean_multiple_12
remainder_scores <-
  ts_mean_multiple_12 %>%
  select(customer_id, date, z_remainder_mean_amount_multiple_12) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

###
### Remainder score
### Create `remainder_score`
###
remainder_scores <-
  remainder_scores %>%
  mutate(combined_remainder_score =
           1 * z_remainder_mean_sum_committed_sub_value_day +
           1 * z_remainder_amount_orders_day +
           1 * z_remainder_sum_item_quantity +
           1 * z_remainder_mean_duplicate_pm +
           1 * z_remainder_mean_multiple_pl +
           1 * z_remainder_mean_amount_multiple_12) %>%
  mutate(z_remainder_score = as.numeric(scale(combined_remainder_score)))
rm(list = ls()[grepl(pattern = '^ts_', x = ls())])

###
### Calculate Penalty scores
###

###
### Penalty score
### `Time/hour of order`
###

# How many subscriptions were made at each hour
## This can be filtered to more recent dates etc.
hour_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(time) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each hour which tells us something about the likelihood of
# 'not good' customers during that hour
overall_time_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, time) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(time, names_from = is_fraud_blacklisted, values_from = Count) %>%
  left_join(hour_count, by = 'time') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(time, contains('_ratio')) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(time, False_ratio) %>%
  rename(time_penalty = "False_ratio")

# Create DF with the mean time penalty (and z-score) for each combination of
# customer_id and date
z_score_mean_time_penalty <-
    data.table(left_join(subscriptions, overall_time_penalty, by = 'time'))[
    ,.(customer_id = first_DT(customer_id),
       time_penalty = first_DT(time_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_time_penalty = mean(time_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_time_penalty = as.numeric(scale(mean_time_penalty)))
rm(hour_count, overall_time_penalty)

###
### Penalty score
### `email_domain`
###
email_domain_count <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    group_by(domain) %>%
    summarise(Count = sum(item_quantity))

overall_domain_penalty <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    group_by(is_fraud_blacklisted, domain) %>%
    summarise(Count = sum(item_quantity)) %>%
    pivot_wider(domain, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(email_domain_count, by = 'domain') %>%
    mutate(False_ratio = False / Count,
           True_ratio = True / Count) %>%
    arrange(desc(Count)) %>%
    mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
    select(domain, False_ratio) %>%
    rename(domain_penalty = "False_ratio") %>%
    mutate(domain_penalty = if_else(!is.finite(domain_penalty), min(domain_penalty), domain_penalty))

domain_group_penalty_domain_level <-
    overall_domain_penalty %>%
    mutate(domain_group = NA) %>%
    mutate(domain_group = if_else(round(domain_penalty, 2) <= 0.83, 0, NA_real_)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.10, 1, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.25, 2, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.65, 3, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) > 1.65, 4, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group), 99, domain_group))

overall_domain_group_penalty <-
    domain_group_penalty_domain_level %>%
    group_by(domain_group) %>%
    summarise(domain_group_penalty = mean(domain_penalty)) %>%
    mutate(domain_group_penalty = if_else(is.na(domain_group_penalty),
                                          median(domain_group_penalty_domain_level$domain_penalty, na.rm = TRUE),
                                          domain_group_penalty))

overall_domain_group_penalty <-
  domain_group_penalty_domain_level %>%
  left_join(overall_domain_group_penalty, by = 'domain_group')

subscriptions_domain_group_penalty <-
    subscriptions %>%
    select(customer_id, order_id, date, email) %>%
    distinct() %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    left_join(overall_domain_group_penalty, by = 'domain')

z_score_mean_domain_group_penalty <-
    data.table(subscriptions_domain_group_penalty)[
    ,.(customer_id = first_DT(customer_id),
       domain_group_penalty = first_DT(domain_group_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_domain_group_penalty = mean(domain_group_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_domain_group_penalty = as.numeric(scale(mean_domain_group_penalty))) %>%
    mutate(z_score_mean_domain_group_penalty = replace_na(z_score_mean_domain_group_penalty, 0))
rm(subscriptions_domain_group_penalty)
rm(email_domain_count, overall_domain_penalty, overall_domain_group_penalty,
   domain_group_penalty_domain_level)

###
### Penalty score
### Postal code (full, 2-digit or 1-digit) penalty
###
# Set thresholds to use full postal code
use_full_pc_threshold_items <- 25
use_full_pc_threshold_customers <- 3
# Set thresholds to use pc2 (first two digit) postal code
use_pc2_threshold_items <- 100
use_pc2_threshold_customers <- 10


# Counts for the full postal code (filter out below thresholds)
pc_count <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           nchar(shipping_pc) %in% 7) %>%
    # Cut off last three characters (empty space and letter combination)
    mutate(shipping_pc = substr(shipping_pc, 1, 4)) %>%
    filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(pc4 = shipping_pc) %>%
    summarise(Count.pc4 = sum(item_quantity),
              d_customers.pc4 = n_distinct(customer_id)) %>%
    filter(Count.pc4 >= use_full_pc_threshold_items,
           d_customers.pc4 >= use_full_pc_threshold_customers)

# Counts for the pc2 code (not in `pc_count` already)
pc2_count <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           nchar(shipping_pc) %in% 7) %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
         !shipping_pc %in% pc_count$pc4) %>%
    mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
    group_by(pc2) %>%
    summarise(Count.pc2 = sum(item_quantity),
              d_customers.pc2 = n_distinct(customer_id)) %>%
    filter(Count.pc2 >= use_pc2_threshold_items,
           d_customers.pc2 >= use_pc2_threshold_customers)

# Counts for the pc1 code (not in `pc_count` or `pc2_counts` already)
pc1_count <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           nchar(shipping_pc) %in% 7) %>%
    # Cut off last three characters (empty space and letter combination)
    mutate(pc1 = substr(shipping_pc, 1, 1),
           pc2 = substr(shipping_pc, 1, 2)) %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           !shipping_pc %in% pc_count$pc4,
           !pc2 %in% pc2_count$pc2) %>%
    group_by(pc1) %>%
    summarise(Count.pc1 = sum(item_quantity))

# Combine pc and pc2 counts
pc_count <-
    subscriptions %>%
    filter(shipping_country %in% c('Netherlands'),
           nchar(shipping_pc) %in% 7,
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    select(shipping_pc) %>%
    distinct() %>%
    mutate(pc1 = substr(shipping_pc, 1, 1),
           pc2 = substr(shipping_pc, 1, 2)) %>%
    rename(pc4 = shipping_pc) %>%
    left_join(pc_count, by = 'pc4') %>%
    left_join(pc1_count, by = 'pc1') %>%
    left_join(pc2_count, by = 'pc2') %>%
    select(pc4, Count.pc4, pc2, Count.pc2, pc1, Count.pc1) %>%
    mutate(pc = if_else(!is.na(Count.pc4), pc4, NA_character_),
           pc = if_else(!is.na(Count.pc2) & is.na(pc), pc2, pc),
           pc = if_else(!is.na(Count.pc1) & is.na(pc), pc1, pc),
           Count = if_else(!is.na(Count.pc4), Count.pc4, NA_real_),
           Count = if_else(!is.na(Count.pc2) & is.na(Count), Count.pc2, Count),
           Count = if_else(!is.na(Count.pc1) & is.na(Count), Count.pc1, Count)) %>%
     select(pc, Count) %>%
     distinct()

# Calculate a 'penalty' for each pc which tells us something about the likelihood of
# 'not good' customers in that pc
overall_pc_penalty <-
    subscriptions %>%
    filter(is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 7) %>%
    mutate(shipping_pc = substr(shipping_pc, 1, 4)) %>%
    # If 4-digit shipping_pc in pc_count$pc use it,
    # If 2-digit shipping_pc in pc_count$pc use that, otherwise use 1-digit shipping_pc
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, NA_character_),
           pc = if_else(substr(shipping_pc, 1, 2) %in% pc_count$pc, substr(shipping_pc, 1, 2), substr(shipping_pc, 1, 1))) %>%
    group_by(is_fraud_blacklisted, pc) %>%
    summarise(Count = sum(item_quantity)) %>%
    pivot_wider(pc, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(pc_count, by = 'pc') %>%
    mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
    select(pc, contains('_ratio')) %>%
    mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
    select(pc, False_ratio) %>%
    rename(pc_penalty = "False_ratio") %>%
    mutate(pc_penalty = if_else(!is.finite(pc_penalty), min(pc_penalty), pc_penalty))

# Create DF with the max pc penalty (and z-score) for each combination of
# customer_id and date
z_score_max_pc_penalty <-
    subscriptions %>%
    filter(nchar(shipping_pc) %in% 7) %>%
    mutate(shipping_pc = substr(shipping_pc, 1, 4)) %>%
    # If 4-digit shipping_pc in pc_count$pc use it,
    # If 2-digit shipping_pc in pc_count$pc use that, otherwise use 1-digit shipping_pc
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, NA_character_),
           pc = if_else(substr(shipping_pc, 1, 2) %in% pc_count$pc, substr(shipping_pc, 1, 2), substr(shipping_pc, 1, 1))) %>%
    left_join(overall_pc_penalty, by = 'pc') %>%
    group_by(customer_id, date) %>%
    summarise(max_pc_penalty = max(pc_penalty)) %>%
    ungroup() %>%
    mutate(z_score_max_pc_penalty = as.numeric(scale(max_pc_penalty)))

###
### Penalty score
### Asset risk category penalty
###
z_score_max_asset_risk_penalty <-
  subscriptions %>%
  ungroup() %>%
  group_by(customer_id, date) %>%
  summarise(max_asset_risk_category_fraud_rate = max(asset_risk_category_fraud_rate)) %>%
  ungroup() %>%
  mutate(z_max_asset_category_penalty = as.numeric(scale(max_asset_risk_category_fraud_rate)))

###
### Penalty score
### `multiple_user_id`
###

# Add 'unique_user_id2' to 'subscriptions' via left join
# Set NA in unique_user_id2 to 0 (zero multiple user_id)
# Round values in 'unique_user_id2':
#  - to 5 over 10
#  - to 10 over 50
#  - to 100 over 100
subscriptions <-
  subscriptions %>%
  left_join(total_logins_dates[, c('customer_id', 'day', 'unique_user_id2')],
            by = c('customer_id', 'day')) %>%
  mutate(unique_user_id2 = replace_na(unique_user_id2, 0),
         unique_user_id2 = as.numeric(unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 5,
                                   round_any(unique_user_id2, 5), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 10,
                                   round_any(unique_user_id2, 25), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 50,
                                   round_any(unique_user_id2, 50), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 100, 100, unique_user_id2))

# How many subscriptions were made for each (rounded) unique multiple user ID amount
## This can be filtered to more recent dates etc.
multiple_user_id_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(unique_user_id2) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each unique multiple user ID amount which tells us something
# about the likelihood of 'not good' customers for that amount
overall_multiple_user_id_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, unique_user_id2) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(id_cols = unique_user_id2, names_from = is_fraud_blacklisted,
              values_from = Count, values_fill = 0) %>%
  left_join(multiple_user_id_count, by = 'unique_user_id2') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(unique_user_id2, contains('_ratio')) %>%
  mutate(unique_user_id2 = as.character(unique_user_id2)) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(unique_user_id2, False_ratio) %>%
  rename(multiple_user_id_penalty = "False_ratio") %>%
  mutate(multiple_user_id_penalty = if_else(!is.finite(multiple_user_id_penalty),
                                            min(multiple_user_id_penalty),
                                            multiple_user_id_penalty))

# Create DF with the mean multiple user ID penalty (and z-score) for each combination of
# customer_id and date
z_score_mean_multiple_user_id_penalty <-
    data.table(left_join(mutate(subscriptions, unique_user_id2 = as.character(unique_user_id2)),
                         # to character for proper treatment (easier join)
                         overall_multiple_user_id_penalty, by = 'unique_user_id2'))[
    ,.(customer_id = first_DT(customer_id),
       multiple_user_id_penalty = first_DT(multiple_user_id_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_multiple_user_id_penalty = mean(multiple_user_id_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_multiple_user_id_penalty = as.numeric(scale(mean_multiple_user_id_penalty)))
rm(total_logins_dates, multiple_user_id_count, overall_multiple_user_id_penalty)

###
### Penalty score
### `payment_category_penalty`
###
# Calculate z-score based on max 'is_fraud_blacklisted_ratio' per user & date
z_payment_category_penalty <-
    subscriptions %>%
    filter(shipping_country %in% 'Netherlands') %>%
    ungroup() %>%
    group_by(customer_id, date) %>%
    summarise(is_fraud_blacklisted_ratio = max(is_fraud_blacklisted_ratio),
              is_fraud_blacklisted = first(is_fraud_blacklisted),
              item_quantity = sum(item_quantity)) %>%
    ungroup() %>%
    mutate(z_payment_category_penalty = as.numeric(scale(is_fraud_blacklisted_ratio))) %>%
    select(customer_id, date, z_payment_category_penalty)

###
### Penalty score
### `nethone_worst_signal`
###

nethone_worst_signal_count <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(nethone_worst_signal) %>%
    summarise(Count = n())

overall_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(is_fraud_blacklisted, nethone_worst_signal) %>%
    summarise(Count = n()) %>%
    pivot_wider(nethone_worst_signal, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(nethone_worst_signal_count, by = 'nethone_worst_signal') %>%
    mutate(False_ratio = False / Count,
           True_ratio = True / Count) %>%
    select(nethone_worst_signal, contains('_ratio')) %>%
    mutate(False_ratio = is_good_customer_ratio / False_ratio,
           True_ratio = is_good_customer_ratio / True_ratio) %>%
    select(nethone_worst_signal, False_ratio) %>%
    rename(nethone_worst_signal_penalty = "False_ratio")

z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal)) %>%
    left_join(overall_nethone_worst_signal_penalty, by = 'nethone_worst_signal') %>%
    group_by(customer_id, date) %>%
    summarise(nethone_worst_signal_penalty = max(nethone_worst_signal_penalty)) %>%
    ungroup() %>%
    mutate(z_score_nethone_worst_signal_penalty = as.numeric(scale(nethone_worst_signal_penalty))) %>%
    mutate(z_score_nethone_worst_signal_penalty = if_else(is.na(z_score_nethone_worst_signal_penalty),
                                                          0, z_score_nethone_worst_signal_penalty))

# Add all customer_id & date combinations of 'subscriptions'
z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    left_join(z_score_nethone_worst_signal_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_nethone_worst_signal_penalty = replace_na(z_score_nethone_worst_signal_penalty, 0)) %>%
    select(customer_id, date, z_score_nethone_worst_signal_penalty) %>%
    distinct()
rm(nethone_worst_signal_count, overall_nethone_worst_signal_penalty)

###
### Combine remainder and penalty scores
### `remainder_scores_penalties`
###
remainder_scores_penalties <-
  remainder_scores %>%
  left_join(z_score_mean_time_penalty[,c('customer_id','date','z_score_mean_time_penalty')],
            by = c('customer_id','date')) %>%
  left_join(z_score_mean_domain_group_penalty, by = c('customer_id','date')) %>%
  left_join(z_score_max_pc_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_score_max_asset_risk_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_score_mean_multiple_user_id_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_payment_category_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_score_nethone_worst_signal_penalty, by = c('customer_id', 'date'))
rm(list = ls()[grepl(pattern = '^z_score_mean', x = ls())])

###
### Create anomaly score
###
anomaly_score <-
  subscriptions %>%
  select(customer_id, date) %>%
  right_join(remainder_scores_penalties, by=c('customer_id','date'))

###
### Apply optimized parameters to calculate anomaly score
###
params <- c(0.172, 0.000, 0.000, 0.044, 0.032, 0.444, 0.231, 0.572, 0.617, 1.722, 0.846, 1.563, 0.732)
params <- c(0.136, 0.000, 0.000, 0.020, 0.000, 0.276, 0.228, 0.433, 0.578, 1.332, 0.456, 1.381, 0.585)

###
### Create optim_scores (optimized anomaly_score)
###
optim_scores <-
  anomaly_score %>%
    mutate(anomaly_score =
           params[1] * z_remainder_mean_sum_committed_sub_value_day +
           params[2] * z_remainder_amount_orders_day +
           params[3] * z_remainder_sum_item_quantity +
           params[4] * z_remainder_mean_duplicate_pm +
           params[5] * z_remainder_mean_multiple_pl +
           params[6] * z_remainder_mean_amount_multiple_12 +
           params[7] * z_score_mean_time_penalty +
           params[8] * z_score_mean_domain_group_penalty +
           params[9] * z_score_max_pc_penalty +
           params[10] * z_max_asset_category_penalty +
           params[11] * z_score_mean_multiple_user_id_penalty +
           params[12] * z_payment_category_penalty +
           params[13] * z_score_nethone_worst_signal_penalty) %>%
  mutate(anomaly_score = as.numeric(scale(anomaly_score)))

# Reduce anomaly_score for good labels from 'hist_labels', re-calculate anomaly_score
optim_scores <-
    optim_scores %>%
    left_join(hist_labels, by = c('customer_id', c('date' = 'day'))) %>%
    mutate(anomaly_score = if_else(label_state %in% c('good'), anomaly_score - 2.5, anomaly_score),
           anomaly_score = if_else(label_state %in% c('uncertain'), anomaly_score - 2, anomaly_score),
           anomaly_score = as.numeric(scale(anomaly_score)))

###
### Create 'anomaly_score_daily' and save to .csv file
###
anomaly_score_daily <-
  optim_scores %>%
  rownames_to_column('id') %>%
  mutate(created_at = now()) %>%
  rename(score_day = date) %>%
  select(created_at, customer_id, score_day, anomaly_score,
         z_remainder_mean_amount_multiple_12, z_remainder_mean_multiple_pl,
         z_remainder_mean_duplicate_pm, z_remainder_mean_sum_committed_sub_value_day,
         z_remainder_amount_orders_day, z_remainder_sum_item_quantity, z_score_mean_time_penalty,
         z_score_mean_domain_group_penalty, z_score_max_pc_penalty,  z_max_asset_category_penalty,
         z_score_mean_multiple_user_id_penalty, z_payment_category_penalty, z_score_nethone_worst_signal_penalty)

# Create anomaly scores on 'order_id' level for the last few days
anomaly_score_order <-
  subscriptions %>%
  select(customer_id, order_id, date) %>%
  filter(date >= today() - 8) %>%
  distinct() %>%
  left_join(select(anomaly_score_daily, customer_id, score_day, anomaly_score),
            by = c('customer_id', 'date' = 'score_day')) %>%
  # Add Onfido bonus
  mutate(anomaly_score = if_else(customer_id %in% recent_customers_onfido_verified,
                                 anomaly_score - 0.5, anomaly_score)) %>%
  select(order_id, anomaly_score) %>%
  mutate(created_at = now())

## Write to `anomaly_score_order_NL.csv`
write.csv(anomaly_score_order, 'anomaly_score_order_NL.csv')

##############
### Spain ####
##############

###
### Preperation
###
# Fresh start / clean workspace
rm(list = ls())

###
### Load data from preparation script and adjust for Spain
###
load('prep_data_all_countries.RData')

# Only keep data from Spain
subscriptions <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain',
           date >= start_date)

# Only keep data from Spain
total_logins_dates <-
    total_logins_dates %>%
    filter(customer_id %in% subscriptions$customer_id,
           day >= start_date)

# Only keep data from Spain; change grouping to 'customer_id' & 'date'
customer_level_ts <-
    customer_level_ts %>%
    filter(shipping_country %in% 'Spain',
           date >= start_date) %>%
    group_by(customer_id, date)

###
### Calculate all metrics for anomaly_score (Spain)
###

# Calculate the means of the metrics used
tibbletime_metrics_means <-
    data.table(customer_level_ts)[date >= start_date & date <= end_date][
        , .(mean_sum_committed_sub_value = mean(sum_committed_sub_value, na.rm=TRUE),
            mean_amount_orders_day = mean(number_orders, na.rm=TRUE),
            mean_sum_item_quantity = mean(sum_item_quantity, na.rm=TRUE),
            mean_duplicate_pm = mean(duplicate_payment_methods, na.rm=TRUE),
            mean_multiple_12 = mean(n_subs_above_12, na.rm=TRUE),
            mean_multiple_pl = mean(n_phone_and_laptops, na.rm=TRUE)),
        keyby=.(date)] %>%
    tibbletime::tbl_time(index = date)

# Overall ratio of 'good' customers (as a numeric, not DF)
## This can be filtered to more recent dates etc.
is_good_customer_ratio <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted) %>%
  summarise(Count = sum(item_quantity)) %>%
  mutate(freq = Count / sum(Count)) %>%
  filter(is_fraud_blacklisted %in% 'False') %>%
  pull(freq)

###
### Remainder score
### `mean_sum_committed_sub_value_day`
###
mean_sum_committed_sub_value_day <-
    select(tibbletime_metrics_means, date, mean_sum_committed_sub_value) %>%
    time_decompose(mean_sum_committed_sub_value, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_sum_committed_sub_value_day <-
  customer_level_ts %>%
  ungroup() %>%
  select(customer_id, date, sum_committed_sub_value, is_fraud_blacklisted) %>%
  left_join(mean_sum_committed_sub_value_day[, c('date', 'season', 'trend')], by = 'date') %>%
  mutate(remainder_mean_sum_committed_sub_value_day = sum_committed_sub_value - season - trend,
         z_remainder_mean_sum_committed_sub_value_day =
           as.numeric(scale(remainder_mean_sum_committed_sub_value_day)))
rm(mean_sum_committed_sub_value_day)

###
### Remainder score
### `amount_orders`
###
mean_amount_orders_day <-
    select(tibbletime_metrics_means, date, mean_amount_orders_day) %>%
    time_decompose(mean_amount_orders_day, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_orders <-
  customer_level_ts %>%
  ungroup() %>%
  select(customer_id, date, number_orders, is_fraud_blacklisted) %>%
  left_join(mean_amount_orders_day[, c('date', 'season', 'trend')], by = 'date') %>%
  mutate(remainder_amount_orders_day = number_orders - season - trend,
         z_remainder_amount_orders_day = as.numeric(scale(remainder_amount_orders_day)))
rm(mean_amount_orders_day)

###
### Remainder score
### `item_quantity`
###
mean_sum_item_quantity <-
    select(tibbletime_metrics_means, date, mean_sum_item_quantity) %>%
    time_decompose(mean_sum_item_quantity, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_sum_item_quantity <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, sum_item_quantity, is_fraud_blacklisted) %>%
    left_join(mean_sum_item_quantity[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_sum_item_quantity = sum_item_quantity - season - trend,
           z_remainder_sum_item_quantity = as.numeric(scale(remainder_sum_item_quantity)))
rm(mean_sum_item_quantity)

###
### Remainder score
### `amount_duplicate_payment_methods`
###
mean_amount_duplicate_pm <-
    select(tibbletime_metrics_means, date, mean_duplicate_pm) %>%
    time_decompose(mean_duplicate_pm, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_duplicate_pm <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, duplicate_payment_methods, is_fraud_blacklisted) %>%
    left_join(mean_amount_duplicate_pm[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_duplicate_pm = duplicate_payment_methods - season - trend,
           z_remainder_mean_duplicate_pm = as.numeric(scale(remainder_mean_duplicate_pm)))
rm(mean_amount_duplicate_pm)

###
### Remainder score
### `amount_subscriptions_above_12`
###
mean_multiple_12 <-
    select(tibbletime_metrics_means, date, mean_multiple_12) %>%
    time_decompose(mean_multiple_12, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_mean_multiple_12 <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_subs_above_12, is_fraud_blacklisted) %>%
    left_join(mean_multiple_12[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_amount_multiple_12 = n_subs_above_12 - season - trend,
           z_remainder_mean_amount_multiple_12 = as.numeric(scale(remainder_mean_amount_multiple_12)))
rm(mean_multiple_12)

###
### Remainder score
### `amount_multiple_phones_laptops`
###
mean_amount_multiple_phones_laptops <-
    select(tibbletime_metrics_means, date, mean_multiple_pl) %>%
    time_decompose(mean_multiple_pl, method = "stl", frequency = "auto", trend = "auto") %>%
    anomalize(remainder, method = "gesd", alpha = 0.05) %>%
    time_recompose() %>%
    select(date, season, trend)

ts_amount_multiple_phones_laptops <-
    customer_level_ts %>%
    ungroup() %>%
    select(customer_id, date, n_phone_and_laptops, is_fraud_blacklisted) %>%
    left_join(mean_amount_multiple_phones_laptops[, c('date', 'season', 'trend')], by = 'date') %>%
    mutate(remainder_mean_multiple_pl = n_phone_and_laptops - season - trend,
           z_remainder_mean_multiple_pl = as.numeric(scale(remainder_mean_multiple_pl)))
rm(mean_amount_multiple_phones_laptops)

###
### `remainder_scores` - Join all remainder score DFs
###
remainder_scores <-
    ts_mean_sum_committed_sub_value_day %>%
    select(customer_id, date, z_remainder_mean_sum_committed_sub_value_day, is_fraud_blacklisted)

## ts_amount_orders
remainder_scores <-
  ts_amount_orders %>%
  select(customer_id, date, z_remainder_amount_orders_day) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_sum_item_quantity
remainder_scores <-
  ts_sum_item_quantity %>%
  select(customer_id, date, z_remainder_sum_item_quantity) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_duplicate_payment_methods
remainder_scores <-
  ts_amount_duplicate_pm %>%
  select(customer_id, date, z_remainder_mean_duplicate_pm) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_amount_multiple_phones_laptops
remainder_scores <-
  ts_amount_multiple_phones_laptops %>%
  select(customer_id, date, z_remainder_mean_multiple_pl) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

## ts_mean_multiple_12
remainder_scores <-
  ts_mean_multiple_12 %>%
  select(customer_id, date, z_remainder_mean_amount_multiple_12) %>%
  right_join(remainder_scores, by = c('customer_id', 'date'))

###
### Remainder score
### Create `remainder_score`
###
remainder_scores <-
  remainder_scores %>%
  mutate(combined_remainder_score =
           1 * z_remainder_mean_sum_committed_sub_value_day +
           1 * z_remainder_amount_orders_day +
           1 * z_remainder_sum_item_quantity +
           1 * z_remainder_mean_duplicate_pm +
           1 * z_remainder_mean_multiple_pl +
           1 * z_remainder_mean_amount_multiple_12) %>%
  mutate(z_remainder_score = as.numeric(scale(combined_remainder_score)))
rm(list = ls()[grepl(pattern = '^ts_', x = ls())])

###
### Calculate Penalty scores
###

###
### Penalty score
### `Time/hour of order`
###

# How many subscriptions were made at each hour
## This can be filtered to more recent dates etc.
hour_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(time) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each hour which tells us something about the likelihood of
# 'not good' customers during that hour
overall_time_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, time) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(time, names_from = is_fraud_blacklisted, values_from = Count) %>%
  left_join(hour_count, by = 'time') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(time, contains('_ratio')) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(time, False_ratio) %>%
  rename(time_penalty = "False_ratio")

# Create DF with the mean time penalty (and z-score) for each combination of
# customer_id and date
z_score_mean_time_penalty <-
    data.table(left_join(subscriptions, overall_time_penalty, by = 'time'))[
    ,.(customer_id = first_DT(customer_id),
       time_penalty = first_DT(time_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_time_penalty = mean(time_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_time_penalty = as.numeric(scale(mean_time_penalty)))
rm(hour_count, overall_time_penalty)

###
### Penalty score
### `email_domain`
###
email_domain_count <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain',
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    group_by(domain) %>%
    summarise(Count = sum(item_quantity))

overall_domain_penalty <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain',
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    group_by(is_fraud_blacklisted, domain) %>%
    summarise(Count = sum(item_quantity)) %>%
    pivot_wider(domain, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(email_domain_count, by = 'domain') %>%
    mutate(False_ratio = False / Count,
           True_ratio = True / Count) %>%
    arrange(desc(Count)) %>%
    mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
    select(domain, False_ratio) %>%
    rename(domain_penalty = "False_ratio") %>%
    mutate(domain_penalty = if_else(!is.finite(domain_penalty), min(domain_penalty), domain_penalty))

domain_group_penalty_domain_level <-
    overall_domain_penalty %>%
    mutate(domain_group = NA) %>%
    mutate(domain_group = if_else(round(domain_penalty, 2) <= 0.83, 0, NA_real_)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.10, 1, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.25, 2, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) <= 1.65, 3, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group) &
                                  round(domain_penalty, 2) > 1.65, 4, domain_group)) %>%
    mutate(domain_group = if_else(is.na(domain_group), 99, domain_group))

overall_domain_group_penalty <-
    domain_group_penalty_domain_level %>%
    group_by(domain_group) %>%
    summarise(domain_group_penalty = mean(domain_penalty)) %>%
    mutate(domain_group_penalty = if_else(is.na(domain_group_penalty),
                                          median(domain_group_penalty_domain_level$domain_penalty, na.rm = TRUE),
                                          domain_group_penalty))

overall_domain_group_penalty <-
  domain_group_penalty_domain_level %>%
  left_join(overall_domain_group_penalty, by = 'domain_group')

subscriptions_domain_group_penalty <-
    subscriptions %>%
    select(customer_id, order_id, date, email) %>%
    distinct() %>%
    mutate(domain = stringi::stri_split_fixed(str = email, pattern = "@", simplify = TRUE)[, 2]) %>%
    left_join(overall_domain_group_penalty, by = 'domain')

z_score_mean_domain_group_penalty <-
    data.table(subscriptions_domain_group_penalty)[
    ,.(customer_id = first_DT(customer_id),
       domain_group_penalty = first_DT(domain_group_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_domain_group_penalty = mean(domain_group_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_domain_group_penalty = as.numeric(scale(mean_domain_group_penalty))) %>%
    mutate(z_score_mean_domain_group_penalty = replace_na(z_score_mean_domain_group_penalty, 0))
rm(subscriptions_domain_group_penalty)
rm(email_domain_count, overall_domain_penalty, overall_domain_group_penalty,
   domain_group_penalty_domain_level)

###
### Penalty score
### Postal code (full or 2-digits) penalty
###
# Set thresholds to use full postal code
use_full_pc_threshold_items <- 50
use_full_pc_threshold_customers <- 5

# Counts for the full postal code (filter out below thresholds)
pc_count <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain',
           is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 5) %>%
    group_by(shipping_pc) %>%
    summarise(Count = sum(item_quantity),
              d_customers = n_distinct(customer_id)) %>%
    filter(Count >= use_full_pc_threshold_items,
           d_customers >= use_full_pc_threshold_customers)

# Counts for the pc2 code (not in `pc_count` already)
pc2_count <-
  subscriptions %>%
  filter(shipping_country %in% 'Spain',
         is_fraud_blacklisted %in% c('True', 'False'),
         nchar(shipping_pc) %in% 5,
         !shipping_pc %in% pc_count$shipping_pc) %>%
  mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
  group_by(pc2) %>%
  summarise(Count = sum(item_quantity))

# Combine pc and pc2 counts
pc_count <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain',
           is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 5) %>%
    select(shipping_pc) %>%
    distinct() %>%
    mutate(pc2 = substr(shipping_pc, 1, 2)) %>%
    left_join(pc_count, by = 'shipping_pc') %>%
    left_join(pc2_count, by = 'pc2', suffix = c('.pc5', '.pc2')) %>%
    mutate(pc = if_else(!is.na(Count.pc5), shipping_pc, pc2),
           Count = if_else(!is.na(Count.pc5), Count.pc5, Count.pc2)) %>%
    select(pc, Count) %>%
    distinct()
rm(pc2_count)

# Calculate a 'penalty' for each pc which tells us something about the likelihood of
# 'not good' customers in that pc
overall_pc_penalty <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain',
           is_fraud_blacklisted %in% c('True', 'False'),
           nchar(shipping_pc) %in% 5) %>%
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, substr(shipping_pc, 1, 2))) %>%
    group_by(is_fraud_blacklisted, pc) %>%
    summarise(Count = sum(item_quantity)) %>%
    pivot_wider(pc, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(pc_count, by = 'pc') %>%
    mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
    select(pc, contains('_ratio')) %>%
    mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
    select(pc, False_ratio) %>%
    rename(pc_penalty = "False_ratio") %>%
    mutate(pc_penalty = if_else(!is.finite(pc_penalty), min(pc_penalty), pc_penalty))

# Create DF with the max pc penalty (and z-score) for each combination of
# customer_id and date
z_score_max_pc_penalty <-
    subscriptions %>%
    filter(nchar(shipping_pc) %in% 5) %>%
    mutate(pc = if_else(shipping_pc %in% pc_count$pc, shipping_pc, substr(shipping_pc, 1, 2))) %>%
    left_join(overall_pc_penalty, by = 'pc') %>%
    group_by(customer_id, date) %>%
    summarise(max_pc_penalty = max(pc_penalty)) %>%
    ungroup() %>%
    mutate(z_score_max_pc_penalty = as.numeric(scale(max_pc_penalty)))

# Add all customer_id & date combinations of 'subscriptions'
# Some shipping_pc might be new or don't have labels yet
z_score_max_pc_penalty <-
    subscriptions %>%
    left_join(z_score_max_pc_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_max_pc_penalty = replace_na(z_score_max_pc_penalty, 0)) %>%
    select(customer_id, date, z_score_max_pc_penalty) %>%
    distinct()
rm(pc_count, overall_pc_penalty)

###
### Penalty score
### Asset risk category penalty
###
z_score_max_asset_risk_penalty <-
  subscriptions %>%
  ungroup() %>%
  group_by(customer_id, date) %>%
  summarise(max_asset_risk_category_fraud_rate = max(asset_risk_category_fraud_rate)) %>%
  ungroup() %>%
  mutate(z_max_asset_category_penalty = as.numeric(scale(max_asset_risk_category_fraud_rate)))

###
### Penalty score
### `multiple_user_id`
###

# Add 'unique_user_id2' to 'subscriptions' via left join
# Set NA in unique_user_id2 to 0 (zero multiple user_id)
# Round values in 'unique_user_id2':
#  - to 5 over 10
#  - to 10 over 50
#  - to 100 over 100
subscriptions <-
  subscriptions %>%
  left_join(total_logins_dates[, c('customer_id', 'day', 'unique_user_id2')],
            by = c('customer_id', 'day')) %>%
  mutate(unique_user_id2 = replace_na(unique_user_id2, 0),
         unique_user_id2 = as.numeric(unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 5,
                                   round_any(unique_user_id2, 5), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 10,
                                   round_any(unique_user_id2, 25), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 50,
                                   round_any(unique_user_id2, 50), unique_user_id2)) %>%
  mutate(unique_user_id2 = if_else(unique_user_id2 > 100, 100, unique_user_id2))

# How many subscriptions were made for each (rounded) unique multiple user ID amount
## This can be filtered to more recent dates etc.
multiple_user_id_count <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(unique_user_id2) %>%
  summarise(Count = sum(item_quantity))

# Calculate a 'penalty' for each unique multiple user ID amount which tells us something
# about the likelihood of 'not good' customers for that amount
overall_multiple_user_id_penalty <-
  subscriptions %>%
  filter(is_fraud_blacklisted %in% c('True', 'False')) %>%
  group_by(is_fraud_blacklisted, unique_user_id2) %>%
  summarise(Count = sum(item_quantity)) %>%
  pivot_wider(id_cols = unique_user_id2, names_from = is_fraud_blacklisted,
              values_from = Count, values_fill = 0) %>%
  left_join(multiple_user_id_count, by = 'unique_user_id2') %>%
  mutate(False_ratio = False / Count,
         True_ratio = True / Count) %>%
  select(unique_user_id2, contains('_ratio')) %>%
  mutate(unique_user_id2 = as.character(unique_user_id2)) %>%
  mutate_if(is.double, function(x) is_good_customer_ratio / x) %>%
  select(unique_user_id2, False_ratio) %>%
  rename(multiple_user_id_penalty = "False_ratio") %>%
  mutate(multiple_user_id_penalty = if_else(!is.finite(multiple_user_id_penalty),
                                            min(multiple_user_id_penalty),
                                            multiple_user_id_penalty))

# Create DF with the mean multiple user ID penalty (and z-score) for each combination of
# customer_id and date
z_score_mean_multiple_user_id_penalty <-
    data.table(left_join(mutate(subscriptions, unique_user_id2 = as.character(unique_user_id2)),
                         # to character for proper treatment (easier join)
                         overall_multiple_user_id_penalty, by = 'unique_user_id2'))[
    ,.(customer_id = first_DT(customer_id),
       multiple_user_id_penalty = first_DT(multiple_user_id_penalty)),
    keyby=.(order_id, date)][
    ,.(mean_multiple_user_id_penalty = mean(multiple_user_id_penalty)), keyby=.(customer_id, date)] %>%
    tibble() %>%
    ungroup() %>%
    arrange(customer_id, date) %>%
    mutate(z_score_mean_multiple_user_id_penalty = as.numeric(scale(mean_multiple_user_id_penalty)))
rm(total_logins_dates, multiple_user_id_count, overall_multiple_user_id_penalty)

###
### Penalty score
### `payment_category_penalty`
###

# Calculate z-score based on max 'is_fraud_blacklisted_ratio' per user & date
z_payment_category_penalty <-
    subscriptions %>%
    filter(shipping_country %in% 'Spain') %>%
    ungroup() %>%
    group_by(customer_id, date) %>%
    summarise(is_fraud_blacklisted_ratio = max(is_fraud_blacklisted_ratio),
              is_fraud_blacklisted = first(is_fraud_blacklisted),
              item_quantity = sum(item_quantity)) %>%
    ungroup() %>%
    mutate(z_payment_category_penalty = as.numeric(scale(is_fraud_blacklisted_ratio)))

###
### Penalty score
### `nethone_worst_signal`
###

nethone_worst_signal_count <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(nethone_worst_signal) %>%
    summarise(Count = n())

overall_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal),
           is_fraud_blacklisted %in% c('True', 'False')) %>%
    group_by(is_fraud_blacklisted, nethone_worst_signal) %>%
    summarise(Count = n()) %>%
    pivot_wider(nethone_worst_signal, names_from = is_fraud_blacklisted, values_from = Count, values_fill = 0) %>%
    left_join(nethone_worst_signal_count, by = 'nethone_worst_signal') %>%
    mutate(False_ratio = False / Count,
           True_ratio = True / Count) %>%
    select(nethone_worst_signal, contains('_ratio')) %>%
    mutate(False_ratio = is_good_customer_ratio / False_ratio,
           True_ratio = is_good_customer_ratio / True_ratio) %>%
    select(nethone_worst_signal, False_ratio) %>%
    rename(nethone_worst_signal_penalty = "False_ratio")

z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    filter(!is.na(nethone_worst_signal)) %>%
    left_join(overall_nethone_worst_signal_penalty, by = 'nethone_worst_signal') %>%
    group_by(customer_id, date) %>%
    summarise(nethone_worst_signal_penalty = max(nethone_worst_signal_penalty)) %>%
    ungroup() %>%
    mutate(z_score_nethone_worst_signal_penalty = as.numeric(scale(nethone_worst_signal_penalty))) %>%
    mutate(z_score_nethone_worst_signal_penalty = if_else(is.na(z_score_nethone_worst_signal_penalty),
                                                          0, z_score_nethone_worst_signal_penalty))

# Add all customer_id & date combinations of 'subscriptions'
z_score_nethone_worst_signal_penalty <-
    subscriptions %>%
    left_join(z_score_nethone_worst_signal_penalty, by = c('customer_id', 'date')) %>%
    mutate(z_score_nethone_worst_signal_penalty = replace_na(z_score_nethone_worst_signal_penalty, 0)) %>%
    select(customer_id, date, z_score_nethone_worst_signal_penalty) %>%
    distinct()
rm(nethone_worst_signal_count, overall_nethone_worst_signal_penalty)

###
### Combine remainder and penalty scores
### `remainder_scores_penalties`
###
remainder_scores_penalties <-
  remainder_scores %>%
  left_join(z_score_mean_time_penalty[,c('customer_id','date','z_score_mean_time_penalty')],
            by=c('customer_id','date')) %>%
  left_join(z_score_mean_domain_group_penalty, by = c('customer_id','date')) %>%
  left_join(z_score_max_pc_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_score_max_asset_risk_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_score_mean_multiple_user_id_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_payment_category_penalty, by = c('customer_id', 'date')) %>%
  left_join(z_score_nethone_worst_signal_penalty, by = c('customer_id', 'date'))
rm(list = ls()[grepl(pattern = '^z_score_mean', x = ls())])

###
### Create anomaly score
###
anomaly_score <-
  subscriptions %>%
  select(customer_id, date, shipping_country) %>%
  right_join(remainder_scores_penalties, by=c('customer_id','date'))
rm(remainder_scores_penalties)

###
### Apply optimized parameters to calculate anomaly score
###
params <- c(0.000, 0.073, 0.000, 0.043, 0.000, 0.377, 0.129, 1.902, 0.794, 1.261, 1.000)
params <- c(0.000, 0.073, 0.000, 0.043, 0.000, 0.377, 0.129, 0.000, 0.000, 1.902, 0.794, 1.261, 1.000)

###
### Create optim_scores (optimized anomaly_score)
###
optim_scores <-
  anomaly_score %>%
    mutate(anomaly_score =
           params[1] * z_remainder_mean_sum_committed_sub_value_day +
           params[2] * z_remainder_amount_orders_day +
           params[3] * z_remainder_sum_item_quantity +
           params[4] * z_remainder_mean_duplicate_pm +
           params[5] * z_remainder_mean_multiple_pl +
           params[6] * z_remainder_mean_amount_multiple_12 +
           params[7] * z_score_mean_time_penalty +
           params[8] * z_score_mean_domain_group_penalty +
           params[9] * z_score_max_pc_penalty +
           params[10] * z_max_asset_category_penalty +
           params[11] * z_score_mean_multiple_user_id_penalty +
           params[12] * z_payment_category_penalty +
           params[13] * z_score_nethone_worst_signal_penalty) %>%
  mutate(anomaly_score = as.numeric(scale(anomaly_score)))

# Only keep Spanish data
optim_scores <-
  optim_scores %>%
  filter(shipping_country %in% 'Spain')

# Reduce anomaly_score for good labels from 'hist_labels', re-calculate anomaly_score
optim_scores <-
    optim_scores %>%
    left_join(hist_labels, by = c('customer_id', c('date' = 'day'))) %>%
    mutate(anomaly_score = if_else(label_state %in% c('good'), anomaly_score - 2, anomaly_score),
           anomaly_score = if_else(label_state %in% c('uncertain'), anomaly_score - 1.5, anomaly_score),
           anomaly_score = as.numeric(scale(anomaly_score)))

###
### Create 'anomaly_score_daily' and save to .csv file
###
anomaly_score_daily <-
  optim_scores %>%
  rownames_to_column('id') %>%
  mutate(created_at = now()) %>%
  rename(score_day = date) %>%
  select(created_at, customer_id, score_day, anomaly_score,
         z_remainder_mean_amount_multiple_12, z_remainder_mean_multiple_pl,
         z_remainder_mean_duplicate_pm, z_remainder_mean_sum_committed_sub_value_day,
         z_remainder_amount_orders_day, z_remainder_sum_item_quantity, z_score_mean_time_penalty,
         z_score_mean_domain_group_penalty, z_score_max_pc_penalty,  z_max_asset_category_penalty,
         z_score_mean_multiple_user_id_penalty, z_payment_category_penalty, z_score_nethone_worst_signal_penalty)

# Create anomaly scores on 'order_id' level for the last few days
anomaly_score_order <-
  subscriptions %>%
  filter(shipping_country %in% 'Spain') %>%
  select(customer_id, order_id, date) %>%
  filter(date >= today() - 8) %>%
  distinct() %>%
  left_join(select(anomaly_score_daily, customer_id, score_day, anomaly_score),
            by = c('customer_id', 'date' = 'score_day')) %>%
  select(order_id, anomaly_score) %>%
  mutate(created_at = now())

## Write to `anomaly_score_order_spain.csv`
write.csv(anomaly_score_order, 'anomaly_score_order_spain.csv')
