
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_group_lvl;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_group_lvl
SELECT
  acct_grp_lev_key,
  acct_lev_desc,
  acct_lev_index,
  field_key,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_group_lvl;
