
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_group;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_group
SELECT
  acct_grp_key,
  acct_grp_lev_key,
  grp_desc,
  dsp_order,
  parent_key,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_group;
