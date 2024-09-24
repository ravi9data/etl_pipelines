DROP TABLE IF EXISTS finance.profit_and_loss_marketing_costs;

CREATE TABLE finance.profit_and_loss_marketing_costs AS 
WITH performance_marketing AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,CASE WHEN b.cost_entity ='B2B-Germany' 
                THEN ROUND((sum(b.amount) OVER (PARTITION BY b.cost_entity)/sum(b.amount) OVER ())*b1.amount)
            ELSE 
                ROUND((sum(b.amount) OVER (PARTITION BY b.cost_entity)/sum(b.amount) OVER ())*(b1.amount)+(COALESCE(b2.amount,0)))
         END AS performance_marketing
    FROM finance.profit_and_loss_input b
    JOIN finance.profit_and_loss_input b1
    ON b.date_=b1.date_
    JOIN finance.profit_and_loss_input b2
    ON b.date_=b2.date_
    WHERE TRUE 
        AND b.dimension='performance_cost'
        AND b1.cost_entity='group_consol' 
        AND b1.dimension ='performance_marketing'
        AND b2.dimension='b2b_freelancers_performance_cost'
)
, branding AS ( 
    SELECT 
        o.date_
        ,o.cost_entity
        ,CASE 
            WHEN o.cost_entity IN ('United States') THEN ius.amount
            WHEN o.cost_entity IN ('Spain') THEN isp.amount
            WHEN o.cost_entity ='B2C-Germany' THEN 
                ROUND((igc.amount -isp.amount-ius.amount)*((o.amount/1000)/(CASE WHEN o.cost_entity ILIKE '%Germany%' THEN
                                                        sum(CASE WHEN o.cost_entity ILIKE '%Germany%' then o.amount/1000 end) OVER() END  )
                                                                    ))
            WHEN o.cost_entity ='B2B-Germany' THEN 
            ROUND((igc.amount -isp.amount-ius.amount)*((o.amount/1000)/(CASE WHEN o.cost_entity ILIKE '%Germany%' THEN
                                                    sum(CASE WHEN o.cost_entity ILIKE '%Germany%' then o.amount/1000 end) OVER() END  )
                                                                ))
            ELSE 0
         END AS branding
    FROM finance.profit_and_loss_redshift o 
    LEFT JOIN finance.profit_and_loss_input igc--GC
        ON o.date_=igc.date_
    LEFT JOIN  finance.profit_and_loss_input isp--spain
        ON o.date_=isp.date_
    LEFT JOIN  finance.profit_and_loss_input ius--US
        ON o.date_=ius.date_
    WHERE TRUE
        AND o.dimension ='gross_subscription_revenue'
        AND igc.dimension='branding'
        AND ius.dimension='branding'
        AND isp.dimension='branding'
        AND igc.cost_entity='group_consol'
        AND isp.cost_entity='Spain'
        AND ius.cost_entity='United States'
)
, partners AS ( 
    SELECT  
        r.date_
        ,r.cost_entity
        ,CASE WHEN r.cost_entity='B2C-Germany' THEN i.amount ELSE 0 END AS partners
    FROM finance.profit_and_loss_redshift r
    LEFT JOIN finance.profit_and_loss_input i
        ON r.date_=i.date_
    WHERE TRUE 
        AND i.dimension ='partners'
        AND r.dimension ='gross_subscription_revenue'
)
, other_marketing_costs AS (
    SELECT  
        r.date_
        ,r.cost_entity
        ,CASE WHEN r.cost_entity='B2C-Germany' THEN i.amount ELSE 0 END AS other_marketing_costs
    FROM finance.profit_and_loss_redshift r
    LEFT JOIN finance.profit_and_loss_input i
        ON r.date_=i.date_
    WHERE TRUE 
        AND i.dimension ='other_marketing_costs'
        AND r.dimension ='gross_subscription_revenue'  
)
,b2b_salaries AS (
    SELECT 
        date_
        ,cost_entity 
        ,round(amount/1000) AS b2b_salaries
    FROM finance.profit_and_loss_input 
    WHERE dimension = 'b2b_salespeople_salaries'
)
, internal_personnel_cost_t_and_d AS (
    SELECT
        i.date_
        ,i.cost_entity
        ,round ((i.amount/1000) * 0.33) AS internal_personnel_cost_t_and_d
    FROM finance.profit_and_loss_input i
    WHERE TRUE 
        AND i.dimension ='tech_salaries'
)    
, external_personnel_cost_t_and_d AS (
    SELECT 
        i.date_
        ,i.cost_entity
        ,round((sum(i.amount)OVER(PARTITION BY i.cost_entity)/sum(i.amount)OVER())*0.33*igc.amount) AS external_personnel_cost_t_and_d
    FROM finance.profit_and_loss_input i
    LEFT JOIN finance.profit_and_loss_input igc
        ON i.date_=igc.date_
    WHERE TRUE 
        AND i.dimension ='tech_salaries'
        AND igc.dimension ='external_personal_costs_tech'
        AND igc.cost_entity ='group_consol'
)    
, tools_infra AS (
    SELECT 
        i.date_
        ,i.cost_entity
        ,round((COALESCE(CASE WHEN igc.cost_entity='group_consol' THEN igc.amount END,0)- 
                COALESCE((CASE WHEN ic.cost_entity='Card' THEN ic.amount END),0))*i1.amount) AS tools_and_infrastructure
    FROM finance.profit_and_loss_input i
    LEFT JOIN finance.profit_and_loss_input i1
        ON i.date_=i1.date_
        AND i.cost_entity=i1.cost_entity
    LEFT JOIN finance.profit_and_loss_input igc
        ON i.date_=igc.date_
    LEFT JOIN finance.profit_and_loss_input ic
        ON i.date_=ic.date_
    WHERE TRUE 
        AND i.dimension ='tech_salaries'
        AND i1.dimension ='tech_and_dev_ratio_split'
        AND igc.dimension ='tools_infrastructure'
        AND igc.cost_entity = 'group_consol'
        AND ic.dimension ='tools_infrastructure'
        AND ic.cost_entity = 'Card'
)    
, risk_data AS (
    SELECT  
        i.date_
        ,i.cost_entity
        ,round((sum(i.amount)OVER(PARTITION BY i.cost_entity)/sum(i.amount)OVER())*0.3*igc.amount)AS risk_data
    FROM finance.profit_and_loss_input i
    LEFT JOIN finance.profit_and_loss_input igc
        ON i.date_=igc.date_
    WHERE TRUE 
        AND i.dimension ='risk_data_cost'
        AND igc.dimension ='risk_data'
        AND igc.cost_entity ='group_consol' 
)    
, other_t_and_d_costs AS (
    SELECT 
        r.date_
        ,r.cost_entity
        ,round((sum(r.amount)OVER(PARTITION BY r.cost_entity)/sum(r.amount)OVER())*igc.amount) AS other_t_and_d_costs
    FROM finance.profit_and_loss_redshift r 
    LEFT JOIN finance.profit_and_loss_input igc
        ON r.date_=igc.date_
    WHERE TRUE 
        AND r.dimension='new_subscriptions'
        AND igc.dimension ='other_tech_dev_costs'
        AND igc.cost_entity ='group_consol'
        AND r.cost_entity NOT IN ('B2B-Germany','Card')
) 
, personnel_costs_non_t_and_d AS (
    SELECT  
        i.date_
        ,i.cost_entity  
        ,round(i.amount/1000*0.67) AS personnel_costs_non_t_and_d
    FROM finance.profit_and_loss_input i
    WHERE TRUE 
        AND i.dimension ='non_tech_salaries'
)
SELECT DISTINCT 
    rs.date_
    ,rs.cost_entity
    ,COALESCE (a.performance_marketing,0) AS performance_marketing
    ,COALESCE (b.branding,0) AS branding
    ,COALESCE (c.partners,0) AS partners
    ,COALESCE (d.other_marketing_costs,0) AS other_marketing_costs
    ,COALESCE (s.b2b_salaries,0) AS b2b_salaries 
    ,COALESCE (e.internal_personnel_cost_t_and_d,0) AS internal_personnel_cost_t_and_d
    ,COALESCE (f.external_personnel_cost_t_and_d,0) AS external_personnel_cost_t_and_d
    ,COALESCE (g.tools_and_infrastructure,0) AS tools_and_infrastructure
    ,COALESCE (h.risk_data,0) AS risk_data
    ,COALESCE (i.other_t_and_d_costs,0) AS other_t_and_d_costs
    ,COALESCE (j.personnel_costs_non_t_and_d,0) AS personnel_costs_non_t_and_d
FROM finance.profit_and_loss_redshift rs
LEFT JOIN performance_marketing a
    ON rs.cost_entity = a.cost_entity
    AND rs.date_ = a.date_
LEFT JOIN branding b
    ON rs.cost_entity = b.cost_entity
    AND rs.date_ = b.date_
LEFT JOIN partners c
    ON rs.cost_entity = c.cost_entity
    AND rs.date_ = c.date_
LEFT JOIN other_marketing_costs d
    ON rs.cost_entity = d.cost_entity
    AND rs.date_ = d.date_
LEFT JOIN b2b_salaries s
    ON rs.cost_entity = s.cost_entity
    AND rs.date_ = s.date_
LEFT JOIN internal_personnel_cost_t_and_d e
    ON rs.cost_entity = e.cost_entity
    AND rs.date_ = e.date_
LEFT JOIN external_personnel_cost_t_and_d f
    ON rs.cost_entity = f.cost_entity
    AND rs.date_ = f.date_
LEFT JOIN tools_infra g
    ON rs.cost_entity = g.cost_entity
    AND rs.date_ = g.date_
LEFT JOIN risk_data h
    ON rs.cost_entity = h.cost_entity
    AND rs.date_ = h.date_
LEFT JOIN other_t_and_d_costs i
    ON rs.cost_entity = i.cost_entity
    AND rs.date_ = i.date_
LEFT JOIN personnel_costs_non_t_and_d j
    ON rs.cost_entity = j.cost_entity
    AND rs.date_ = j.date_
WHERE rs.cost_entity IN ('B2C-Germany', 'Austria', 'Spain', 'Netherlands', 'B2B-Germany', 'United States')
;


GRANT SELECT ON finance.profit_and_loss_marketing_costs TO tableau;