
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_Acct_Dist_Acct;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_Acct_Dist_Acct
SELECT
  acct_key,
  branch_key,
  id,
  system_primary,
  active,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_Acct_Dist_Acct;
