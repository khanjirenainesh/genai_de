
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_Prod_Chk_Distribution
WHERE
  start_date IN (
    SELECT DISTINCT
      TO_DATE(SUBSTRING(start_date, 0, 23), 'DD/MM/YYYY HH:MI:SS AM')
    FROM au_sdl.sdl_perenso_Prod_Chk_Distribution
  );

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_Prod_Chk_Distribution
SELECT
  acct_key,
  prod_key,
  CAST(TO_TIMESTAMP(SUBSTRING(start_date, 0, 23), 'DD/mm/yyyy HH:mi:ss %p') AS TIMESTAMPNTZ) AS start_date,
  CAST(TO_TIMESTAMP(SUBSTRING(end_date, 0, 23), 'DD/mm/yyyy HH:mi:ss %p') AS TIMESTAMPNTZ) AS end_date,
  in_distribution,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_Prod_Chk_Distribution;
