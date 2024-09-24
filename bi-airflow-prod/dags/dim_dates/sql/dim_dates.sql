DROP TABLE IF EXISTS dim_dates_temp;
CREATE TEMP TABLE dim_dates_temp AS
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1
    FROM numbers
    WHERE n < 365  -- Generating dates for the next 1 year
), 
date_series AS (
    SELECT current_date + (n - 1) AS datum
    FROM numbers
), 
month_end_dates AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day' AS end_of_month
)
SELECT 
    ds.datum, 
    EXTRACT(YEAR FROM ds.datum) AS year_number, 
    EXTRACT(WEEK FROM ds.datum) AS year_week_number, 
    CAST(EXTRACT(YEAR FROM ds.datum) AS VARCHAR) || '-' || LPAD(CAST(EXTRACT(WEEK FROM ds.datum) AS VARCHAR), 2, '0') AS week_number, 
    DATE_PART('doy', ds.datum) AS day_of_year, 
    EXTRACT(QUARTER FROM ds.datum) AS qtr_number, 
    EXTRACT(MONTH FROM ds.datum) AS month_number, 
    TO_CHAR(ds.datum, 'Month') AS month_name, 
    EXTRACT(DAY FROM ds.datum) AS month_day_number, 
    EXTRACT(DOW FROM ds.datum) AS week_day_number, 
    TO_CHAR(ds.datum, 'Day') AS day_name, 
    CASE 
        WHEN EXTRACT(DOW FROM ds.datum) IN (0, 6) THEN 0 
        ELSE 1
    END AS day_is_weekday, 
    CASE 
        WHEN EXTRACT(DAY FROM ds.datum) = 1 THEN 1 
        ELSE 0 
    END AS day_is_first_of_month, 
    CASE 
        WHEN ds.datum = (SELECT end_of_month FROM month_end_dates) THEN 1 
        ELSE 0 
    END AS day_is_last_of_month 
FROM 
    date_series ds 
ORDER BY 
    ds.datum;
   
  
 INSERT INTO public.dim_dates 
 SELECT * FROM dim_dates_temp 
 WHERE datum > (
 SELECT max(datum) FROM public.dim_dates
 )
