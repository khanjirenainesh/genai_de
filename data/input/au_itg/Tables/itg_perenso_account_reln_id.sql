
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_reln_id;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_reln_id
SELECT
  acct_key,
  field_key,
  id,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_reln_id;
