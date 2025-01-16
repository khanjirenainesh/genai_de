
 --================statement 1================ 
INSERT INTO au_itg.itg_perenso_product_group_lvl
SELECT
  prod_grp_lev_key,
  prod_lev_desc,
  prod_lev_index,
  field_key,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_product_group_lvl;
