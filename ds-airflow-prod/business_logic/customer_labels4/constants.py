COUNTRIES_LIST = ["Germany", "Netherlands", "Spain", "Austria"]

RULESET = {
    'us_b2c_v0': [
        'u1', 'u2', 'u3',
        'g1', 'g2', 'g3',
        'g4', 'g5', 'g6',
        'g7', 'g8', 'g9',
        'g10', 'f1', 'c1',
        'f2', 'c2', 'g11',
        'g12', 'g13', 'c3',
        'f3', 'f4', 'c4',
        'c5', 'f5', 'f6',
        'g14', 'g15', 'f7',
        'c6', 'g16', 'f8',
        'u4',
    ]
}

RULES = {
    'u1' : ('(n_payments==-9) & (label.isna())','unknown_no_payments'),
    # Uncertain revocations
    'u2':(
         '(label.isna())'
        '& (n_revocations == n_subscriptions)''','uncertain_revocation'
    ),
     # Uncertain: all refunds or unknown cases or not delivered cases
    'u3': (
        '(label.isna()) '
        '& ((n_payments - n_refunded_payments == 0) | ((paid_not_refunded_payments == 1) & (delivered_assets == 0)) |'
        ' ((net_revenue_paid <= 5) & (((current_dpd ==0) & (outstanding_assets == 0)) | ((paid_rate < 0) & (n_dc_subscriptions == -9)))))'
        '','uncertain'
    ),
    # Excellent customers
    'g1' : (
        '(avg_payment_dpd <= 5) & '
        '(time_since_first_payment>75) & '
        '(net_revenue_paid+asset_amount_paid > 250) & '
        '( '
        '    (n_payments > 5) | '
        '    ((n_payments > 3) & (net_revenue_paid+asset_amount_paid > 500)) '
        ') & '
        '( '
        '    (current_dpd ==0) | '
        '    (current_dpd >0) & (outstanding_amount <= 5) '
        ') & '
        '(max_payment_dpd <= 10) ''','good_premium'
    ),
    # Excellent customers that are not active atm (meaning that all due payments happened and are paid)
    'g2' : (
        '(outstanding_amount==0) & '
        '(time_to_first_failed_payment==-9) & '
        '(net_revenue_paid+asset_amount_paid > 250) & '
        '(outstanding_assets==0) & '
        '(n_active_subscriptions==0) & '
        '(avg_payment_dpd <= 5) & (paid_not_refunded_payments > 1)''','good_premium'
    ),
    # Excellent customers , at least 5 payments that didn't fail in the past
    'g3' : (
        '(label.isna()) '
        '& (net_revenue_paid+asset_amount_paid > 250) '
        '& (((outstanding_amount ==0) & (avg_payment_dpd <=5)) '
        ' )'#'| ((time_since_last_payment < 45) & ((n_payments - paid_not_refunded_payments)/n_subscriptions <= 1) & (avg_payment_dpd < 6)))'
        '& (n_payments - n_past_failed_payments > 5) '
        '& (n_past_failed_payments <= 1) '
        '& (time_since_first_payment>75) ''', 'good_premium'
    ),
    # Good customers with revenue between 30 and 250€
    'g4' : (
        '(label.isna()) & '
        '(time_since_first_payment>90) & '
        '(current_dpd ==0) & '
        '(max_payment_dpd <= 5) & '
        '(net_revenue_paid <= 250) & (net_revenue_paid > 30) & '
        '(paid_rate >= 0.98) & '
        '(total_refunded_amount == 0) ''','good'
    ),
    # Good customers with revenue between 30 and 250€, without using max_dpd which is not always reliable due to the new infra issues with re-try payments.
    'g5' : (
        '(time_since_first_payment>90) & '
        '(outstanding_amount ==0) & '
        '(avg_payment_dpd < 10) & '
        '(net_revenue_paid <= 250) & (net_revenue_paid > 30) & '
        '(n_revocations == 0) & '
        '(label.isna())''', 'good'
    ),
    # Avg dpd <10 and at least 75 days old (more than 4 payments OR up to 3 but ended)
    'g6' : (
            '(label.isna()) '
            '& (paid_rate > 0.98) '
            '& (time_since_first_payment >= 75) '
            '& ((n_payments - n_refunded_payments) / n_subscriptions > 1) '
            '& ('
            '    ((n_payments  <= 3) & (time_since_last_payment > 31) & (n_dc_subscriptions == -9) & (avg_payment_dpd <= 10) & (outstanding_assets < 1))| '
            '    ((n_payments  > 4) & (n_dc_subscriptions == -9) & (avg_payment_dpd <= 10)) |'
            '    ((time_since_first_payment > 90) & (net_revenue_paid > 500) & (avg_payment_dpd < 15))) '
            '','good'
    ),
    # Too early to give them a label, but they didn't show suspicious payments behaviour.
    'g7' : (
        '(label.isna()) '
        '& (paid_rate >= 0.95) '# 98
        '& (net_revenue_paid < 400)'
        '& (((time_since_first_payment <= 75) '
        '& (time_since_last_payment <= 31) '
        '& (net_revenue_paid > 10) '
        '& (n_past_failed_payments ==0)) '
         '| ((time_since_first_payment >= 75) & (n_payments <= 3) & (avg_payment_dpd <= 10) & (n_dc_subscriptions == -9))) '
        '& (n_payments / n_subscriptions > 1)''','good_potential'
    ),

    # Good customers who can miss payments sometimes but recover quite fast.
    'g8' : (
            '(label.isna()) '
            '& (avg_payment_dpd <= 5) '
            '& (net_revenue_paid > 100) '
            '& (time_since_last_payment <= 40) '
            '& (time_since_first_payment > 70) '
            '& (outstanding_amount < 150) '
            '& (paid_rate > 0.87)''','good'
    ),
    # Good customers who paywith some delay but not critical to be considered unstable
    'g9' : (
            '(label.isna()) '
            '& (((avg_payment_dpd <= 3) '
            '& (net_revenue_paid >= 250) '
            '& (current_dpd < 30)) '
            ' | ((net_revenue_paid >= 800)) & ((avg_payment_dpd <= 10)))'
            '& (paid_rate > 0.9) '
            '& (n_dc_subscriptions == -9)''','good'
    ),
    # Customers with dpd between 5 and 10, with unconstant behaviour who paid a good amount of money
    'g10' : (
            '(label.isna()) '
            '& ('
            '((avg_payment_dpd >= 10) & (n_payments > 3) & (paid_rate > 0.9)) |'
            '((avg_payment_dpd >= 5) & (net_revenue_paid >= 250) & (paid_rate > 0.9)) |'
            '((avg_payment_dpd > 7) & (net_revenue_paid > 500) & (avg_payment_dpd < 45) ) |'
            '((avg_payment_dpd >= 15) & (net_revenue_paid > 500) & (paid_rate > 0.98) & (time_since_first_payment > 90)) |'
            '((avg_payment_dpd >= 10) & (n_payments > 7) & (time_since_last_payment < 60) & (avg_payment_dpd < 40) & (net_revenue_paid >= 80))'
            ')'
            '& (paid_rate > 0.8) '
            '& ((net_revenue_paid + asset_amount_paid) < 1000)'
            '& (outstanding_dc_assets <= 0)''','good_unstable'
    ),

     # customer paid only the first payment for X assets, disappeared after and is in DC now
    'f1' : (
            '(label.isna()) '
            '& ((n_payments / n_subscriptions) >= 3) '
            '& ((paid_not_refunded_payments / n_subscriptions) <= 1) '
            '& ((n_dc_subscriptions >= 1) |'
            ' ((n_dc_subscriptions < 1) & (outstanding_assets > 0) & (time_since_last_payment >= 120)))'
        '','fraud'
    ),
    # credit default: bad paid_rate, asset returned, no active subscription
    'c1': (
            '(label.isna()) '
            '& (paid_rate <= 0.8)'
            '& (paid_rate > 0.35)'
            '& (time_since_first_payment > 90)'
            '& (outstanding_dc_assets < 1)'
            '& (n_active_subscriptions < 1) & (n_payments > n_paid_payments)''','credit_default'
    ),
    # asset outstanding / in DC, at most two paid payments,
    # more than 2 months since last payment, nothing purchased
    'f2': (
            '(label.isna())'
            '& (paid_rate <= 0.5)'
            #'& ('
            #    '(n_active_subscriptions == 0) |'
            #    '((n_active_subscriptions > 0) & (time_between_first_and_last_subscription > 0)))'
            '& ('
                '(n_dc_subscriptions >= 1)'
                '| (outstanding_assets >= 1))'
            '& ((paid_not_refunded_payments / n_subscriptions) <= 2)'
            '& (time_since_last_payment >= 90)''','fraud' #100
            #'& (asset_amount_paid < 100)' #
    ),

    # At least 4 not refunded payments per subscription, all assets returned, at leat 3 months since last payment
    'c2': (
            '(label.isna())'
            '& (paid_rate <= 0.7)'
            '& ((paid_not_refunded_payments / n_subscriptions) >= 4)'
            '& (time_since_last_payment >= 120)'
            '& ((outstanding_assets <= 0) | (outstanding_dc_assets <= 0))''','credit_default'
    ),

    # Unstable good, borderline with default
    'g11': (
      '(label.isna()) '
        '& (paid_rate > 0.67)'
        '& (n_dc_subscriptions == -9) '
        '& (avg_payment_dpd > 5)'
        '& (time_since_last_payment<= 45) '
        '& (time_to_first_failed_payment > 35)'
        '& ((n_paid_payments > 5) | (net_revenue_paid >=100))'
        '& (n_past_failed_payments>=2)'
        '& (n_payments - n_paid_payments <= 2)''','good_unstable'
    ),

    # previously was part of 'credit_default_suspect' (rule_c7)
    # now closely matches 'rule_g19':
    # active subscription, no DC, several paid payments, decent paid_rate, less than 90 days avg_payment_dpd, etc.
    'g12': (
        '(label.isna())'
        '& (paid_rate > 0.67)'
        '& (n_active_subscriptions >= 1)'
        '& (n_dc_subscriptions == -9)'
        '& (outstanding_assets > 0)'
        '& ((paid_not_refunded_payments / n_subscriptions) > 1)'
        '& ((n_payments - n_paid_payments <= 4) & (n_payments > 10))'
        '& (avg_payment_dpd < 40)'
        '& (avg_payment_dpd > 3)' #new
        '& (time_since_last_payment<= 45)'
        '& (time_to_first_failed_payment > 35)'
        '& (net_revenue_paid >=100)'
        '& (asset_amount_paid < 100)''','good_unstable'
    ),

    'g13': (
       '(label.isna()) '
        '& (((paid_rate >= 0.8) & (n_active_subscriptions > 0) & (time_since_last_payment < 45)) | ((n_active_subscriptions == 0) & (paid_rate >= 0.95)))'
        '& (avg_payment_dpd <= 7)'
        '& ((net_revenue_paid>= 200)| ((time_since_first_payment >= 90) & (paid_not_refunded_payments > 2) & (delivered_assets > 0)))''','good'
    ),

    # Defaulting customers with at least 1 active subscription.
    # Also, defaulting customers, who might have recovered most of the payments, but that defaulted for long time periods and would have been considered defaulting customers in the past.
    # Better to keep them default instead of converting to good or good_unstable.
    'c3' : (
        '(label.isna()) '
        '& (paid_rate > 0.5)'
        '& (n_dc_subscriptions == -9) '
        '& (avg_payment_dpd > 5)'
        '& (time_since_last_payment> 45) '
        '& (n_past_failed_payments>2)' #
        '& (n_payments - n_paid_payments > 2)''','credit_default'
        ),
    # Active subscription, no DC, but at least 3 months since the last payment
    # OR Already in DC, but active subscriptions left, assets not returned yet and at leasst 3 months since last payment
    'f3': (
            '(label.isna())'
            #'& (n_active_subscriptions >= 1)'
            '& (time_since_last_payment >= 90)'
            '& (paid_not_refunded_payments / n_subscriptions < 10)'
            '& ((((outstanding_assets > 0) | (outstanding_dc_assets > 0)) & (n_dc_subscriptions > 0) & (paid_rate <= 0.6)) |'
            '((paid_rate <= 0.5) & (asset_amount_paid < 100) & (n_dc_subscriptions <= 0) & (n_active_subscriptions >= 1) & (time_between_first_and_last_payment < 120)))'
            '','fraud'
    ),
    # Group of customers which
    # 1) either have lots of missed payments but also paid a somewhat recent payment (Indicator of excessive amount of subscriptions)
    # 2) or recently started to fail payments but too early to label them as either fraud or CD
    # 3) already failed second payment and suspicious
    'f4': (
        '(label.isna())'
        '& (((paid_rate <= 0.5)'
        '& (n_active_subscriptions >= 1)'
        '& (n_dc_subscriptions <= 0)'
        '& ((paid_not_refunded_payments / n_subscriptions) > 1)'
        '& ((outstanding_assets > 0) | (outstanding_dc_assets > 0))'
        '& (time_since_last_payment < 90)'
        '& (avg_payment_dpd < 90) & (avg_payment_dpd > 5)'
        '& (paid_not_refunded_payments / n_subscriptions < 4)'
        '& (asset_amount_paid < 100))'
        '| ('
        '((time_since_first_payment < 60) | ((time_between_first_and_last_payment < 40) & (time_since_first_payment < 90))) '
        '& (paid_rate < 0.6) & (paid_not_refunded_payments > 0) & (current_dpd > 3)))''','fraud_suspect'
    ),
    #Active customers, with mor than 1 payment per subscription and at least 2 failed payments, and without paying for at least 90 days
    'c4': (
        '(label.isna())'
        '& (paid_rate > 0.5)'
        '& (paid_rate <= 0.8)'
        '& (n_active_subscriptions >= 1)'
        '& (n_dc_subscriptions <= 0)'
        '& ((paid_not_refunded_payments / n_subscriptions) > 1)'
        '& ((n_payments - paid_not_refunded_payments) / n_subscriptions >= 2)' #5
        '& (outstanding_assets > 0)'
        '& (time_since_last_payment < 90)'
        '& (avg_payment_dpd > 5)','credit_default'
    ),


    # Likely defaulting customers, either in dc or with enough failed payments to be considered defaulting
    'c5':(
        '(label.isna())'
        '& (n_payments - paid_not_refunded_payments > 1)'
        '& (paid_not_refunded_payments / n_subscriptions > 3)'
        '& (avg_payment_dpd > 5)' #new
        '& ('
        '((n_dc_subscriptions > 0) & (n_paid_payments > 5))'
        '| ((time_since_first_payment > 75) & (asset_amount_paid <=0) & (n_past_failed_payments > 1) & (time_since_last_payment > 45))'
        '| ((n_dc_subscriptions > 0) & (paid_rate >= 0.8) & (avg_payment_dpd >= 45))'
        ')''','credit_default'
    ),
    #DC, failed amount, outstanding assets
    'f5': (
        '(label.isna())'
        '& (paid_rate <= 0.8)'
        #'& (time_since_last_payment < 120)'
        '& (n_dc_subscriptions >= 1)'
        '& ((outstanding_assets > 0) | (outstanding_dc_assets > 0))'
        '& (asset_amount_paid < 10)''','fraud'
    ),

    # Suspicious, but payments behaviour suggests defaulting rather than fraud (several payments, paid with delay, etc.)
    'f6' : (
        '(label.isin(["fraud"])) & ((paid_not_refunded_payments / n_subscriptions > 6) & (net_revenue_paid >= 120))','fraud_suspect'
    ),

    #Max 1 failed payment per sub, at least 4 paid payments, more than 3 months history, not more than 3 past failed/delayed payments
    'g14':(
        '(label.isna())'
        '& (paid_rate > 0.7)'
        '& ((n_payments - n_paid_payments) / n_active_subscriptions <= 1)'
        '& (paid_not_refunded_payments > 3)'
        '& (avg_payment_dpd < 10)'
        '& (time_since_first_payment >= 90)'
        '& (n_past_failed_payments < 3)','good'
    ),
    #Max 1 failed payment per sub, at least 4 paid payments, more than 3 months history, some delay with payments
    'g15':(
        '(label.isna())'
        '& (paid_rate > 0.7)'
        '& ((n_payments - n_paid_payments) / n_active_subscriptions <= 1)'
        '& (paid_not_refunded_payments > 3)'
        '& (avg_payment_dpd >= 10)'
        '& (time_since_first_payment >= 90)','good_unstable'
    ),

    # Delineate fraud_suspects at the second payment
    'f7':(
        '((label.isin(["fraud_suspect","uncertain"])) | (label.isna()))'
        '& ((n_payments - n_refunded_payments)/ n_active_subscriptions == 2) '
        '& (outstanding_assets > 0)'
        '& (current_dpd >= 5)'
        '& (n_paid_payments == n_active_subscriptions)','fraud_suspect_2nd_payment'

    ),
    'c6':(
        '(label.isin(["fraud"]))'
        '& (responsiveness.fillna("-9").str.contains("yes")) '
        '& (time_since_last_payment < 90)'
        '& (paid_rate > 0.35)','credit_default'
    ),
    'g16':(
        '((label.isin(["uncertain"])) | (label.isna()))'
        '& (failed_transaction_reasons_new=="insufficient_funds") '
        '& (time_since_last_payment < 90)'
        '& (n_payments > 3) '
        '& (net_revenue_paid > 1000)'
        '& (n_payments - paid_not_refunded_payments < 8)'
        '& (paid_rate > 0.7)','good_unstable'
    ),
    'f8':(
        '(label.isin(["fraud"]))'
        '& (time_between_first_and_last_payment <= 70) '
        '& (n_paid_payments / n_subscriptions <= 3)','fraud_early'
    ),

    # All other cases
    'u4' : (
        '(label.isna())','uncertain'
    )
}

LABEL_TRANSACTIONS_DICT = {
    'insufficient':'insufficient_funds',
    'invalid':'invalid',
    'blocked':'blocked_card',
    'closed':'closed_account',
    'expired':'expired_card',
    'limit':'limit_reached',
    'fraud':'fraud_suspect',
}

FILL_NA_0 = [
    'current_dpd',
    'asset_amount_paid',
    'delivered_assets',
    'n_refunded_payments'
]

FILL_NA_9 = [
    'n_dc_subscriptions',
    'outstanding_dc_assets',
    'outstanding_assets',
    'time_to_first_failed_payment',
    'time_since_last_payment',
    'n_payments',
    'outstanding_amount',
    'n_subscriptions',
]
