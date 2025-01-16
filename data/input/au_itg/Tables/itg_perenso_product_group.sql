
 --================statement 1================ 
INSERT INTO au_itg.itg_perenso_product_group
SELECT
  prod_grp_key,
  prod_grp_lev_key,
  prod_grp_desc,
  dsp_order,
  parent_key,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_product_group;
