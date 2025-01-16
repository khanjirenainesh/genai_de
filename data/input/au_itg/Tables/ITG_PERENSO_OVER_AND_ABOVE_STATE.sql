
 --================statement 1================ 
DELETE FROM AU_ITG.ITG_PERENSO_OVER_AND_ABOVE_STATE
WHERE
  (STORE_CHK_HDR_KEY, OVER_AND_ABOVE_KEY) IN (
    SELECT DISTINCT
      SPSR.STORE_CHK_HDR_KEY,
      SPSR.OVER_AND_ABOVE_KEY
    FROM AU_SDL.SDL_PERENSO_OVER_AND_ABOVE_STATE AS SPSR
  );

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_Over_and_Above_State
SELECT
  SPSR.over_and_above_key,
  CAST(TO_TIMESTAMP(SUBSTRING(SPSR.start_date, 0, 23), 'DD/mm/yyyy HH:mi:ss %p') AS TIMESTAMPNTZ) AS start_date,
  CAST(TO_TIMESTAMP(SUBSTRING(SPSR.end_date, 0, 23), 'DD/mm/yyyy HH:mi:ss %p') AS TIMESTAMPNTZ) AS end_date,
  SPSR.batch_count,
  SPSR.store_chk_hdr_key,
  SPSR.run_id,
  SPSR.create_dt
FROM au_sdl.sdl_perenso_Over_and_Above_State AS SPSR;
