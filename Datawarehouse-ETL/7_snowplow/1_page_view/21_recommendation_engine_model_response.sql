CREATE TEMP TABLE recommendation_engine_model_response_temp AS
WITH numbers AS ( 
  	SELECT * FROM public.numbers WHERE ordinal < 15
)
, rec AS (
    SELECT 
    	tracing_id,
        json_extract_array_element_text(recommendations, numbers.ordinal::int,true) as rec_json
    FROM s3_spectrum_kafka_topics_raw.recommendation_response_v1 rec
    CROSS JOIN numbers
    WHERE numbers.ordinal < json_array_length(rec.recommendations, true)
	  AND rec.kafka_received_at::date >= DATEADD('day', -7, current_date)
)
SELECT
	tracing_id,
	LEFT(json_extract_path_text(rec_json, 'product_sku'), 
		POSITION('V' IN json_extract_path_text(rec_json, 'product_sku')) - 1) AS product_sku,
	json_extract_path_text(rec_json, 'is_recommended_by_model') AS is_recommended_by_model,
	json_extract_path_text(rec_json, 'model_rank') AS model_rank,
	json_extract_path_text(rec_json, 'rank') AS rank_,
	json_extract_path_text(rec_json, 'model_probability') AS model_probability
FROM rec;

BEGIN TRANSACTION;
DELETE FROM dm_risk.recommendation_engine_model_response
USING recommendation_engine_model_response_temp  tmp
WHERE recommendation_engine_model_response.tracing_id = tmp.tracing_id  ;

INSERT INTO dm_risk.recommendation_engine_model_response
SELECT *
FROM recommendation_engine_model_response_temp ;

END TRANSACTION;

DROP TABLE recommendation_engine_model_response_temp ;