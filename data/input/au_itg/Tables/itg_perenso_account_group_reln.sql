
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_group_reln;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_group_reln
SELECT
  acct_key,
  acct_grp_key,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_group_reln;
