
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_type;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_type
SELECT
  acct_type_key,
  acct_type_desc,
  dsp_order,
  active,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_type;
