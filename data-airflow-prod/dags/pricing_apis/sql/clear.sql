DELETE FROM pricing.upcs_asin
WHERE "date" = CURRENT_DATE;

DELETE FROM pricing.ean_asin
WHERE "date" = CURRENT_DATE;
