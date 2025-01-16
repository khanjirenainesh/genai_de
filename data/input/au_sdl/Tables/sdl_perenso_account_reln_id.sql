
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_account_reln_id;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_account_reln_id
SELECT
  *
FROM au_wks.wks_perenso_account_reln_id;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_account_reln_id SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_account_reln_id
SELECT
  *
FROM au_sdl.sdl_perenso_account_reln_id;
