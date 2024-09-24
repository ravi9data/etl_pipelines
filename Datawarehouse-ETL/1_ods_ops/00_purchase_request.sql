drop table if exists ods_production.purchase_request;
create table ods_production.purchase_request as 
WITH a AS (
         SELECT request."Name" AS request_id,       --purchase requests
            request."CreatedDate" AS request_date,
               s.request_partner,
            item."Name" AS purchase_request_item_id,
            item.effective_quantity__c - COALESCE(item.delivered__c, 0::double precision) AS quantity,
            request.status__c AS request_status,
            p.sku_variant__c AS variant_sku,
            p.sku_product__c AS product_sku
           FROM stg_salesforce.purchase_request_item__c item
             LEFT JOIN stg_salesforce.purchase_request__c request ON request."Id"::text = item.purchase_request__c::text
             LEFT JOIN ods_production.supplier s ON request.supplier__c::text = s.supplier_id::text
             LEFT JOIN stg_salesforce."Product2" p ON p."Id"::text = item.variant__c::text
          WHERE (request.status__c in ('PURCHASED','PURCHASE','IN DELIVERY')) AND NOT request."IsDeleted"
          ORDER BY request."CreatedDate" DESC
        ), a2 AS (
         SELECT a.variant_sku,               --purchase requests grouping
            sum(
                CASE
                    WHEN a.request_partner::text = 'Media Markt'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_mm,
            sum(
                CASE
                    WHEN a.request_partner::text = 'Saturn'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_saturn,
            sum(
                CASE
                    WHEN a.request_partner::text = 'Conrad'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_conrad,
            sum(
                CASE
                    WHEN a.request_partner::text = 'Gravis'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_gravis,
            sum(
                CASE
                    WHEN a.request_partner::text = 'Quelle'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_Quelle,
             sum(
                CASE
                    WHEN a.request_partner::text = 'Weltbild'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_weltbild,
             sum(
                CASE
                    WHEN a.request_partner::text = 'Aldi Talk'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_alditalk,
            sum(
                CASE
                    WHEN a.request_partner::text = 'Comspot'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_comspot,
            sum(
                CASE
                    WHEN a.request_partner::text = 'Samsung'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_Samsung,  
            sum(
                CASE
                    WHEN a.request_partner::text = 'Shifter'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_shifter,
            sum(
                CASE
                    WHEN a.request_partner::text = 'iRobot'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_irobot,          
            sum(
                CASE
                    WHEN a.request_partner::text = 'Others'::text THEN a.quantity
                    ELSE 0::double precision
                END) AS requested_others
           FROM a
          GROUP BY a.variant_sku
        ), b AS (            
         SELECT DISTINCT              --Subscriptions by stores
            s."Id" AS subscription_id,
            s.status__c AS subscription_status,
            s.allocation_status__c AS allocation_status,
            i.f_product_sku_variant__c AS variant_sku,
            i.f_product_sku_product__c AS product_sku,
                CASE
                    WHEN st.account_name in ('Media Markt','Saturn','Conrad','Gravis','Aldi Talk','Weltbild','Samsung', 'UNITO','Comspot','Shifter','iRobot') THEN st.account_name
                    ELSE 'Others'::text
                END AS account_name
           FROM stg_salesforce.subscription__c s
             LEFT JOIN stg_salesforce."OrderItem" i ON s.order_product__c::text = i."Id"::text
             LEFT JOIN stg_salesforce."Order" o ON o."Id"::text = s.order__c::text
             LEFT JOIN ods_production.store st ON st.id::double precision = o.store_id__c
          WHERE true 
           AND s.status__c::text = 'ACTIVE'::text 
           AND s.allocation_status__c::text = 'PENDING ALLOCATION'::text 
           AND st.store_type = 'online'::text        
        ), b2 AS (
         SELECT b.variant_sku,
            count(DISTINCT
                CASE
                    WHEN b.account_name = 'Media Markt'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_mm,
            count(DISTINCT
                CASE
                    WHEN b.account_name = 'Saturn'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_saturn,
            count(DISTINCT
                CASE
                    WHEN b.account_name = 'Conrad'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_conrad,
            count(DISTINCT
                CASE
                    WHEN b.account_name = 'Gravis'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_gravis,
           count(DISTINCT
                CASE
                    WHEN b.account_name = 'UNITO'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_unito,    
          count(DISTINCT
                CASE
                    WHEN b.account_name = 'Aldi Talk'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_alditalk, 
          count(DISTINCT
                CASE
                    WHEN b.account_name = 'Weltbild'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_weltbild,   
           count(DISTINCT
                CASE
                    WHEN b.account_name = 'Comspot'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_comspot,
           count(DISTINCT
                CASE
                    WHEN b.account_name = 'Samsung'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_samsung, 
           count(DISTINCT
                CASE
                    WHEN b.account_name = 'Shifter'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_shifter, 
            count(DISTINCT
                CASE
                    WHEN b.account_name = 'iRobot'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_irobot,      
           count(DISTINCT
                CASE
                    WHEN b.account_name = 'Others'::text THEN b.subscription_id
                    ELSE NULL::character varying
                END) AS pending_allocation_others
           FROM b
          GROUP BY b.variant_sku
        ), c AS (
         SELECT a."Id" AS asset_id,
            a."Status" AS asset_status,
              s.request_partner AS supplier,
            a.f_product_sku_variant__c AS variant_sku,
            a.f_product_sku_product__c AS product_sku,
            a.final_condition__c as condition
           FROM stg_salesforce."Asset" a
             LEFT JOIN ods_production.supplier s 
              ON a.supplier__c::text = s.supplier_id::text
          WHERE a."Status"::text = 'IN STOCK'::text 
           and a.warehouse__c in ('synerlogis_de', 'ups_softeon_eu_nlrng', 'ingram_micro_eu_flensburg')
        ), c2 AS (
         SELECT c.variant_sku,
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Media Markt'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_mm,
          count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Media Markt'::text 
                    and condition = 'NEW' THEN c.asset_id
                    ELSE NULL::character varying
           		END) AS assets_stock_mm_new,
           count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Media Markt'::text
					and condition = 'AGAN' THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_mm_agan,
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Saturn'::text 
                      THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_saturn,
                 count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Saturn'::text 
                     and condition = 'NEW' THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_saturn_new ,
                 count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Saturn'::text 
                     and condition = 'AGAN' THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_saturn_agan,
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Conrad'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_conrad,
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Gravis'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_gravis,
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Aldi Talk'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_alditalk,
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Quelle'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_quelle,
             count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Weltbild'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_weltbild,   
                    count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Comspot'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_comspot,
                count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Samsung'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_samsung, 
                count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Shifter'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_shifter, 
                count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'iRobot'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_irobot, 
            count(DISTINCT
                CASE
                    WHEN c.supplier::text = 'Others'::text THEN c.asset_id
                    ELSE NULL::character varying
                END) AS assets_stock_others
           FROM c
          GROUP BY c.variant_sku
       ),   d as (     ---orders
 				select 
	 				ord.order_id,
	 				ord.created_date::DATE,
	 				CASE
            		WHEN st.account_name in ('Media Markt','Saturn','Conrad','Gravis','Aldi Talk','Weltbild','UNITO','Samsung','Comspot','Shifter','iRobot') THEN st.account_name
            			ELSE 'Others'::text
      			END AS account_name,
      			ord.status,
 				ord.initial_scoring_decision,
 				o.variant_sku,
 				v.ean,
 				o.quantity
	 				from ods_production."order" ord
	 			left join ods_production.order_item o on o.order_id=ord.order_id
 	 			left join ods_production.store st on ord.store_id=st.id
     			left join ods_production.variant v on v.variant_sku=o.variant_sku
     			where status = 'MANUAL REVIEW' and initial_scoring_decision = 'APPROVED'),
                d2 AS (
 				 SELECT d.variant_sku,
            sum(
                CASE
                    WHEN d.account_name = 'Media Markt'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_mm,
            sum(
                CASE
                    WHEN d.account_name = 'Saturn'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_saturn,
            sum(
                CASE
                    WHEN d.account_name = 'Conrad'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_conrad,
            sum(
                CASE
                    WHEN d.account_name = 'Gravis'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_gravis,
           sum(
                CASE
                    WHEN d.account_name = 'UNITO'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_UNITO, 
           sum(
                CASE
                    WHEN d.account_name = 'Aldi Talk'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_alditalk,
           sum(
                CASE
                    WHEN d.account_name = 'Weltbild'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_weltbild,    
            sum(
                CASE
                    WHEN d.account_name = 'Comspot'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_comspot,
            sum(
                CASE
                    WHEN d.account_name = 'Samsung'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_samsung,
            sum(
                CASE
                    WHEN d.account_name = 'Shifter'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_shifter,
             sum(
                CASE
                    WHEN d.account_name = 'iRobot'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_irobot,    
            sum(
                CASE
                    WHEN d.account_name = 'Others'::text THEN quantity
                    ELSE 0
                END) AS approved_pending_manual_review_others
           FROM d
          GROUP BY d.variant_sku
        ),v AS (
         SELECT DISTINCT 
            COALESCE(v_1.variant_sku, a.f_product_sku_variant__c::character varying) AS variant_sku,
          	COALESCE(p.product_sku,a.f_product_sku_product__c::character varying) AS product_sku, 
            listagg(DISTINCT v_1.ean, ' | '::text) AS ean,
            listagg(DISTINCT p2.name::text, ', '::text) AS variant_name,
            listagg(DISTINCT p.product_name::text, ', '::text) AS product_name
           FROM ods_production.variant v_1
             left join ods_production.product p on p.product_id=v_1.product_id
             left join stg_salesforce.product2 p2 on p2.sku_variant__c=v_1.variant_sku 
             LEFT JOIN stg_salesforce."Asset" a ON v_1.variant_sku::text = a.f_product_sku_variant__c
          GROUP BY (COALESCE(v_1.variant_sku, a.f_product_sku_variant__c::character varying)), (COALESCE(p.product_sku,a.f_product_sku_product__c::character varying))          
          ), 
		s AS (
         SELECT DISTINCT 
            a.asset_status_original as status_original,
            a.asset_status_grouped as status_new
           FROM ods_production.asset a
           where a.asset_status_grouped in ('IN STOCK','ON RENT','REFURBISHMENT','TRANSITION')
        ), mm AS (
         SELECT a.f_product_sku_variant__c AS variant_sku,
            count(DISTINCT
                CASE
                    WHEN s.status_new = 'IN STOCK'::text THEN a."Id"
                    ELSE NULL::character varying
                END) AS assets_in_stock,
            count(DISTINCT
                CASE
                    WHEN s.status_new in ('IN STOCK','ON RENT','REFURBISHMENT','TRANSITION') THEN a."Id"
                    ELSE NULL::character varying
                END) AS assets_on_book
           FROM stg_salesforce."Asset" a
             LEFT JOIN s ON a."Status"::text = s.status_original::text
          WHERE a.supplier_name__c = 'Media Markt'::text and a.warehouse__c in ('synerlogis_de','ups_softeon_eu_nlrng', 'ingram_micro_eu_flensburg')
          GROUP BY a.f_product_sku_variant__c
        )
 SELECT DISTINCT 
    v.variant_sku,
    v.product_sku,
    v.ean,
    coalesce(v.variant_name,v.product_name) AS variant_name,
	    COALESCE(b2.pending_allocation_mm, 0::bigint) + 
	   	COALESCE(b2.pending_allocation_saturn, 0::bigint) + 
	    COALESCE(b2.pending_allocation_conrad, 0::bigint) + 
	    COALESCE(b2.pending_allocation_gravis, 0::bigint) + 
	    COALESCE(b2.pending_allocation_unito, 0::bigint) +
	    COALESCE(b2.pending_allocation_weltbild, 0::bigint) +
	    COALESCE(b2.pending_allocation_alditalk, 0::bigint) +
	    COALESCE(b2.pending_allocation_comspot, 0::bigint) +
        COALESCE(b2.pending_allocation_samsung, 0::bigint) +
        COALESCE(b2.pending_allocation_shifter, 0::bigint) +
        COALESCE(b2.pending_allocation_irobot, 0::bigint) +
	    COALESCE(b2.pending_allocation_others, 0::bigint) 
	  AS pending_allocation_total,
	    COALESCE(c2.assets_stock_mm, 0::bigint) + 
	    COALESCE(c2.assets_stock_saturn, 0::bigint) + 
	    COALESCE(c2.assets_stock_conrad, 0::bigint) + 
	    COALESCE(c2.assets_stock_gravis, 0::bigint) + 
	    COALESCE(c2.assets_stock_alditalk, 0::bigint) + 
	    COALESCE(c2.assets_stock_quelle, 0::bigint) + 
	    COALESCE(c2.assets_stock_weltbild, 0::bigint) + 
	    COALESCE(c2.assets_stock_comspot, 0::bigint) +
        COALESCE(c2.assets_stock_samsung, 0::bigint) +
        COALESCE(c2.assets_stock_shifter, 0::bigint) +
        COALESCE(c2.assets_stock_irobot, 0::bigint) +
	    COALESCE(c2.assets_stock_others, 0::bigint)
	   AS assets_stock_total,
	    COALESCE(a2.requested_mm, 0::double precision) +
	    COALESCE(a2.requested_saturn, 0::double precision) +
	    COALESCE(a2.requested_conrad, 0::double precision) + 
	    COALESCE(a2.requested_gravis, 0::double precision) + 
	    COALESCE(a2.requested_quelle, 0::double precision) + 
	    COALESCE(a2.requested_weltbild, 0::double precision) + 
	    COALESCE(a2.requested_alditalk, 0::double precision) + 
	    COALESCE(a2.requested_comspot, 0::double precision)  +
        COALESCE(a2.requested_samsung, 0::double precision)  +
        COALESCE(a2.requested_shifter, 0::double precision)  +
        COALESCE(a2.requested_irobot, 0::double precision)  +
	    COALESCE(a2.requested_others, 0::double precision) 
	    AS requested_total,
	  	COALESCE(d2.approved_pending_manual_review_mm, 0::bigint) +
	  	COALESCE(d2.approved_pending_manual_review_saturn, 0::bigint) + 
	  	COALESCE(d2.approved_pending_manual_review_conrad, 0::bigint) + 
	  	COALESCE(d2.approved_pending_manual_review_gravis, 0::bigint) + 
	  	COALESCE(d2.approved_pending_manual_review_UNITO, 0::bigint) + 
	  	COALESCE(d2.approved_pending_manual_review_weltbild, 0::bigint) +
	  	COALESCE(d2.approved_pending_manual_review_alditalk, 0::bigint) +
	  	COALESCE(d2.approved_pending_manual_review_comspot, 0::bigint) +
        COALESCE(d2.approved_pending_manual_review_samsung, 0::bigint) + 
        COALESCE(d2.approved_pending_manual_review_shifter, 0::bigint) + 
        COALESCE(d2.approved_pending_manual_review_irobot, 0::bigint) + 
	  	COALESCE(d2.approved_pending_manual_review_others, 0::bigint) 
	  	AS approved_pending_manual_review__total,  
    COALESCE(b2.pending_allocation_mm, 0::bigint) AS pending_allocation_mm,
    COALESCE(c2.assets_stock_mm, 0::bigint) AS assets_stock_mm,
    COALESCE(c2.assets_stock_mm_new, 0::bigint) AS assets_stock_mm_new,
    COALESCE(c2.assets_stock_mm_agan, 0::bigint) AS assets_stock_mm_agan,
    COALESCE(mm.assets_on_book, 0::bigint) AS assets_book_mm,
    COALESCE(a2.requested_mm, 0::double precision) AS requested_mm,
    COALESCE(d2.approved_pending_manual_review_mm, 0::bigint) AS approved_pending_manual_review_mm,
    COALESCE(b2.pending_allocation_saturn, 0::bigint) AS pending_allocation_saturn,
    COALESCE(c2.assets_stock_saturn, 0::bigint) AS assets_stock_saturn,
    COALESCE(c2.assets_stock_saturn_new, 0::bigint) AS assets_stock_saturn_new,
    COALESCE(c2.assets_stock_saturn_agan, 0::bigint) AS assets_stock_saturn_agan,
    COALESCE(a2.requested_saturn, 0::double precision) AS requested_saturn,
    COALESCE(d2.approved_pending_manual_review_saturn, 0::bigint) AS approved_pending_manual_review_saturn,
    COALESCE(b2.pending_allocation_conrad, 0::bigint) AS pending_allocation_conrad,
    COALESCE(c2.assets_stock_conrad, 0::bigint) AS assets_stock_conrad,
    COALESCE(a2.requested_conrad, 0::double precision) AS requested_conrad,
    COALESCE(d2.approved_pending_manual_review_conrad, 0::bigint) AS approved_pending_manual_review_conrad,
    COALESCE(b2.pending_allocation_gravis, 0::bigint) AS pending_allocation_gravis,
    COALESCE(c2.assets_stock_gravis, 0::bigint) AS assets_stock_gravis,
    COALESCE(a2.requested_gravis, 0::double precision) AS requested_gravis,
    COALESCE(d2.approved_pending_manual_review_gravis, 0::bigint) AS approved_pending_manual_review_gravis,    
    COALESCE(b2.pending_allocation_UNITO, 0::bigint) AS pending_allocation_UNITO,
    COALESCE(c2.assets_stock_quelle, 0::bigint) AS assets_stock_quelle,
    COALESCE(a2.requested_quelle, 0::double precision) AS requested_quelle,
    COALESCE(d2.approved_pending_manual_review_UNITO, 0::bigint) AS approved_pending_manual_review_UNITO,
    COALESCE(b2.pending_allocation_weltbild, 0::bigint) AS pending_allocation_weltbild,
    COALESCE(c2.assets_stock_weltbild, 0::bigint) AS assets_stock_weltbild,
    COALESCE(a2.requested_weltbild, 0::double precision) AS requested_weltbild,
    COALESCE(d2.approved_pending_manual_review_weltbild, 0::bigint) AS approved_pending_manual_review_weltbild,  
    COALESCE(b2.pending_allocation_alditalk, 0::bigint) AS pending_allocation_alditalk,
    COALESCE(c2.assets_stock_alditalk, 0::bigint) AS assets_stock_alditalk,
    COALESCE(a2.requested_alditalk, 0::double precision) AS requested_alditalk,
    COALESCE(d2.approved_pending_manual_review_alditalk, 0::bigint) AS approved_pending_manual_review_alditalk, 
    COALESCE(b2.pending_allocation_comspot, 0::bigint) as pending_allocation_comspot,
    COALESCE(c2.assets_stock_comspot, 0::bigint) as assets_stock_comspot,
    COALESCE(a2.requested_comspot, 0::double precision)  as requested_comspot,
    COALESCE(d2.approved_pending_manual_review_comspot, 0::bigint) as approved_pending_manual_review_comspot,
    COALESCE(b2.pending_allocation_shifter, 0::bigint) as pending_allocation_shifter,
    COALESCE(c2.assets_stock_shifter, 0::bigint) as assets_stock_shifter,
    COALESCE(a2.requested_shifter, 0::double precision)  as requested_shifter,
    COALESCE(d2.approved_pending_manual_review_shifter, 0::bigint) as approved_pending_manual_review_shifter,
    COALESCE(b2.pending_allocation_irobot, 0::bigint) as pending_allocation_irobot,
    COALESCE(c2.assets_stock_irobot, 0::bigint) as assets_stock_irobot,
    COALESCE(a2.requested_irobot, 0::double precision)  as requested_irobot,
    COALESCE(d2.approved_pending_manual_review_irobot, 0::bigint) as approved_pending_manual_review_irobot,
    COALESCE(b2.pending_allocation_samsung, 0::bigint) as pending_allocation_samsung,
    COALESCE(c2.assets_stock_samsung, 0::bigint) as assets_stock_samsung,
    COALESCE(a2.requested_samsung, 0::double precision)  as requested_samsung,
    COALESCE(d2.approved_pending_manual_review_samsung, 0::bigint) as approved_pending_manual_review_samsung,
    COALESCE(b2.pending_allocation_others, 0::bigint) AS pending_allocation_others,
    COALESCE(c2.assets_stock_others, 0::bigint) AS assets_stock_others,
    COALESCE(a2.requested_others, 0::double precision) AS requested_others,
    COALESCE(d2.approved_pending_manual_review_others, 0::bigint) AS approved_pending_manual_review_others
   FROM v
     LEFT JOIN a2 ON v.variant_sku::text = a2.variant_sku::text
     LEFT JOIN c2 ON v.variant_sku::text = c2.variant_sku
     LEFT JOIN d2 ON v.variant_sku::text = d2.variant_sku
     LEFT JOIN b2 ON v.variant_sku::text = b2.variant_sku::text
     LEFT JOIN mm ON mm.variant_sku = b2.variant_sku::text;