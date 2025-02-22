
 --================statement 1================ 
DELETE   AU_EDW.EDW_PERENSO_ACCOUNT_METCASH_DIM;

 --================statement 2================ 
INSERT INTO AU_EDW.EDW_PERENSO_ACCOUNT_METCASH_DIM (
  ACCT_METCASH_ID,
  ACCT_ID,
  ACCT_DISPLAY_NAME,
  ACCT_TYPE_DESC,
  ACCT_STREET_1,
  ACCT_STREET_2,
  ACCT_STREET_3,
  ACCT_SUBURB,
  ACCT_POSTCODE,
  ACCT_PHONE_NUMBER,
  ACCT_FAX_NUMBER,
  ACCT_EMAIL,
  ACCT_COUNTRY,
  ACCT_REGION,
  ACCT_STATE,
  ACCT_BANNER_COUNTRY,
  ACCT_BANNER_DIVISION,
  ACCT_BANNER_TYPE,
  ACCT_BANNER,
  ACCT_TYPE,
  ACCT_SUB_TYPE,
  ACCT_GRADE,
  ACCT_NZ_PHARMA_COUNTRY,
  ACCT_NZ_PHARMA_STATE,
  ACCT_NZ_PHARMA_TERRITORY,
  ACCT_NZ_GROC_COUNTRY,
  ACCT_NZ_GROC_STATE,
  ACCT_NZ_GROC_TERRITORY,
  ACCT_SSR_COUNTRY,
  ACCT_SSR_STATE,
  ACCT_SSR_TEAM_LEADER,
  ACCT_SSR_TERRITORY,
  ACCT_SSR_SUB_TERRITORY,
  ACCT_IND_GROC_COUNTRY,
  ACCT_IND_GROC_STATE,
  ACCT_IND_GROC_TERRITORY,
  ACCT_IND_GROC_SUB_TERRITORY,
  ACCT_AU_PHARMA_COUNTRY,
  ACCT_AU_PHARMA_STATE,
  ACCT_AU_PHARMA_TERRITORY,
  ACCT_AU_PHARMA_SSR_COUNTRY,
  ACCT_AU_PHARMA_SSR_STATE,
  ACCT_AU_PHARMA_SSR_TERRITORY,
  ACCT_STORE_CODE,
  ACCT_FAX_OPT_OUT,
  ACCT_EMAIL_OPT_OUT,
  ACCT_CONTACT_METHOD
)
SELECT
  RELN.ID AS ACCT_METCASH_ID,
  EDW.ACCT_ID,
  EDW.ACCT_DISPLAY_NAME,
  EDW.ACCT_TYPE_DESC,
  EDW.ACCT_STREET_1,
  EDW.ACCT_STREET_2,
  EDW.ACCT_STREET_3,
  EDW.ACCT_SUBURB,
  EDW.ACCT_POSTCODE,
  EDW.ACCT_PHONE_NUMBER,
  EDW.ACCT_FAX_NUMBER,
  EDW.ACCT_EMAIL,
  EDW.ACCT_COUNTRY,
  EDW.ACCT_REGION,
  EDW.ACCT_STATE,
  EDW.ACCT_BANNER_COUNTRY,
  EDW.ACCT_BANNER_DIVISION,
  EDW.ACCT_BANNER_TYPE,
  EDW.ACCT_BANNER,
  EDW.ACCT_TYPE,
  EDW.ACCT_SUB_TYPE,
  EDW.ACCT_GRADE,
  EDW.ACCT_NZ_PHARMA_COUNTRY,
  EDW.ACCT_NZ_PHARMA_STATE,
  EDW.ACCT_NZ_PHARMA_TERRITORY,
  EDW.ACCT_NZ_GROC_COUNTRY,
  EDW.ACCT_NZ_GROC_STATE,
  EDW.ACCT_NZ_GROC_TERRITORY,
  EDW.ACCT_SSR_COUNTRY,
  EDW.ACCT_SSR_STATE,
  EDW.ACCT_SSR_TEAM_LEADER,
  EDW.ACCT_SSR_TERRITORY,
  EDW.ACCT_SSR_SUB_TERRITORY,
  EDW.ACCT_IND_GROC_COUNTRY,
  EDW.ACCT_IND_GROC_STATE,
  EDW.ACCT_IND_GROC_TERRITORY,
  EDW.ACCT_IND_GROC_SUB_TERRITORY,
  EDW.ACCT_AU_PHARMA_COUNTRY,
  EDW.ACCT_AU_PHARMA_STATE,
  EDW.ACCT_AU_PHARMA_TERRITORY,
  EDW.ACCT_AU_PHARMA_SSR_COUNTRY,
  EDW.ACCT_AU_PHARMA_SSR_STATE,
  EDW.ACCT_AU_PHARMA_SSR_TERRITORY,
  EDW.ACCT_STORE_CODE AS ACCT_STORE_CODE,
  EDW.ACCT_FAX_OPT_OUT,
  EDW.ACCT_EMAIL_OPT_OUT,
  EDW.ACCT_CONTACT_METHOD
FROM AU_EDW.EDW_PERENSO_ACCOUNT_DIM AS EDW, AU_ITG.ITG_PERENSO_ACCOUNT_RELN_ID AS RELN, AU_ITG.ITG_PERENSO_ACCOUNT_FIELDS AS FIELDS
WHERE
  EDW.ACCT_ID = RELN.ACCT_KEY
  AND RELN.FIELD_KEY = FIELDS.FIELD_KEY
  AND UPPER(FIELDS.FIELD_DESC) = 'METCASH ID';
