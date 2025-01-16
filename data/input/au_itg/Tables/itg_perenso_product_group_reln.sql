
 --================statement 1================ 
INSERT INTO au_itg.itg_perenso_product_group_reln
SELECT
  prod_key,
  prod_grp_key,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_product_group_reln;
