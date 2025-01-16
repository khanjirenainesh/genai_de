
 --================statement 1================ 
DELETE FROM AU_ITG.ITG_PERENSO_SURVEY_RESULT
WHERE
  (COALESCE(STORE_CHK_HDR_KEY, '999999'), COALESCE(LINE_KEY, '999999'), COALESCE(TODO_KEY, '999999'), COALESCE(PROD_GRP_KEY, '999999')) IN (
    SELECT DISTINCT
      COALESCE(SPSR.STORE_CHK_HDR_KEY, '999999'),
      COALESCE(SPSR.LINE_KEY, '999999'),
      COALESCE(SPSR.TODO_KEY, '999999'),
      COALESCE(SPSR.PROD_GRP_KEY, '999999')
    FROM AU_SDL.SDL_PERENSO_SURVEY_RESULT AS SPSR
  );

 --================statement 2================ 
DELETE FROM AU_ITG.ITG_PERENSO_SURVEY_RESULT
WHERE
  store_chk_hdr_key IN (
    SELECT DISTINCT
      store_chk_hdr_key
    FROM AU_EDW.EDW_PERENSO_SURVEY
    WHERE
      store_chk_date >= CURRENT_TIMESTAMP() - 91
  );

 --================statement 3================ 
INSERT INTO au_itg.itg_perenso_survey_result
SELECT
  SPSR.store_chk_hdr_key,
  SPSR.line_key,
  SPSR.todo_key,
  SPSR.prod_grp_key,
  SPSR.optionans,
  SPSR.notesans,
  SPSR.run_id,
  SPSR.create_dt
FROM au_sdl.sdl_perenso_survey_result AS SPSR;
