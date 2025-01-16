
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_custom_list;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_custom_list
SELECT
  acct_key,
  field_key,
  option_desc,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_custom_list;
