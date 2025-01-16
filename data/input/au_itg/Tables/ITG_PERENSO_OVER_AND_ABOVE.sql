
 --================statement 1================ 
DELETE FROM AU_ITG.ITG_PERENSO_OVER_AND_ABOVE
WHERE
  (OVER_AND_ABOVE_KEY, ACCT_KEY, PROD_GRP_KEY) IN (
    SELECT DISTINCT
      SPSR.OVER_AND_ABOVE_KEY,
      SPSR.ACCT_KEY,
      SPSR.PROD_GRP_KEY
    FROM AU_SDL.SDL_PERENSO_OVER_AND_ABOVE AS SPSR
  );

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_Over_and_Above
SELECT
  SPSR.over_and_above_key,
  SPSR.acct_key,
  SPSR.todo_option_key,
  SPSR.prod_grp_key,
  SPSR.activated,
  SPSR.notes,
  SPSR.run_id,
  SPSR.create_dt
FROM au_sdl.sdl_perenso_Over_and_Above AS SPSR;
