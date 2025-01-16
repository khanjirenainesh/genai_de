
 --================statement 1================ 
TRUNCATE TABLE AU_SDL.SDL_METCASH_IND_GROCERY;

 --================statement 2================ 
INSERT INTO AU_SDL.SDL_METCASH_IND_GROCERY
SELECT
  *
FROM AU_WKS.WKS_SDL_METCASH_IND_GROCERY
WHERE
  FILE_NAME = '"+(String)globalMap.get("f")+"';

 --================statement 3================ 
/* load to RAW table */
INSERT INTO AU_SDL.SDL_RAW_METCASH_IND_GROCERY
SELECT
  *
FROM AU_SDL.SDL_METCASH_IND_GROCERY;
