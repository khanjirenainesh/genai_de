
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_Acct_Dist_Acct;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_Acct_Dist_Acct
SELECT
  *
FROM au_wks.wks_perenso_Acct_Dist_Acct;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_Acct_Dist_Acct SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_Acct_Dist_Acct
SELECT
  *
FROM au_sdl.sdl_perenso_Acct_Dist_Acct;
