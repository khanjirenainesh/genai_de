
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_account;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_account
SELECT
  *
FROM au_wks.wks_perenso_account;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_account SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_account
SELECT
  *
FROM au_sdl.sdl_perenso_account;
