
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account
SELECT
  acct_key,
  disp_name,
  acct_type_key,
  active,
  acct_street1,
  acct_street2,
  acct_street3,
  acct_suburb,
  acct_postcode,
  acct_statecode,
  acct_state,
  acct_country,
  acct_phone,
  acct_fax,
  acct_email,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account;
