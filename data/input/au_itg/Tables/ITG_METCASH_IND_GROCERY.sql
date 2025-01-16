
 --================statement 1================ 
/* Delete ITG based on week dates, so any file over lapping week dates will be overwritten and this wont cause issues */
DELETE FROM AU_ITG.ITG_METCASH_IND_GROCERY
WHERE
  WEEK_END_DT IN (
    SELECT DISTINCT
      WEEK_END_DT
    FROM AU_WKS.WKS_ITG_METCASH_IND_GROCERY
  );

 --================statement 2================ 
INSERT INTO AU_ITG.ITG_METCASH_IND_GROCERY
SELECT
  *,
  CURRENT_TIMESTAMP()
FROM AU_WKS.WKS_ITG_METCASH_IND_GROCERY;

 --================statement 3================ 
/* delete data in wks sdl */
DELETE FROM AU_WKS.WKS_SDL_METCASH_IND_GROCERY
WHERE
  FILE_NAME = '"+(String)globalMap.get("f")+"';
