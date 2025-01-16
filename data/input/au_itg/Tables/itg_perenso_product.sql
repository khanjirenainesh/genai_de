
 --================statement 1================ 
INSERT INTO au_itg.itg_perenso_product
SELECT
  prod_key,
  prod_id,
  prod_desc,
  prod_ean,
  active,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_product;
