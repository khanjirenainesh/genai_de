

CREATE TABLE rg_edw.edw_custgp_cogs_automation 
AS
(
WITH copa
AS
(
---Fetch COGS and Free Goods from COPA-----
SELECT DISTINCT fisc_yr,
       fisc_yr_per,
       cmp.ctry_group,
       cogs.co_cd,
       prft_Ctr,
       (CASE 
        WHEN sls_org IS NULL THEN 
          (CASE 
            WHEN cogs.co_cd = '4130' THEN '2100' 
            WHEN cogs.co_cd = '8266' THEN '320S' 
            WHEN cogs.co_cd = '4481' THEN '2210' 
            ELSE sls_org 
            END) 
        WHEN sls_org = '2400' AND LTRIM(matl_num,'0') NOT IN ('41812332','41802332') 
             '2500' 
        ELSE sls_org 
        END),
       obj_crncy_co_obj,
       LTRIM(cust_num,'0') AS cust_num,
       LTRIM(matl_num,'0') AS matl_num,
       CASE
         WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(amt_obj_crncy)
         ELSE 0
       END AS nts,
       CASE
         WHEN cogs.acct_hier_shrt_desc = 'FG' THEN SUM(amt_obj_crncy)
         ELSE 0
       END AS freegood_amt,
       CASE
         WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(round(qty))
         ELSE 0
       END AS nts_volume
FROM rg_edw.edw_copa_trans_fact cogs
LEFT JOIN rg_edw.edw_company_dim cmp ON cogs.co_cd = cmp.co_cd
INNER  rg_itg.itg_custgp_cogs_fg_control fgctl on 
													   cogs.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and 	
													   cogs.co_cd = fgctl.co_cd and 
													   cogs.fisc_yr_per >= fgctl.valid_from and
													   cogs.fisc_yr_per < fgctl.valid_to and 
													   case when cogs.acct_hier_shrt_desc = 'FG' 
													        then ltrim(cogs.acct_num,'0') = fgctl.gl_acct_num 
															when cogs.acct_hier_shrt_desc = 'NTS' 
													        then '0' = fgctl.gl_acct_num end and
													   fgctl.active = 'Y'													   

WHERE   cogs.fisc_yr::CHARACTER VARYING::TEXT >= 2023 and cogs.acct_hier_shrt_desc in ('NTS','FG')
GROUP BY fisc_yr,
         fisc_yr_per,
         cogs.co_cd,
         cmp.ctry_group,
         prft_ctr,
         sls_org,
         obj_crncy_co_obj,
         LTRIM(cust_num,'0'),
         LTRIM(matl_num,'0'),
         cogs.acct_hier_shrt_desc
 ),
stdc
AS
(
select DISTINCT (CASE WHEN bwkey = '2400' AND LTRIM(matnr,'0') NOT IN ('41812332','41802332') THEN '2500' 
                  ELSE bwkey END) as bwkey,
				  ltrim(matnr,'0') as matnr,
				  nvl(nullif(lfgja||'0'||lpad(lfmon,2,'0'),'0000'),'1900001') as valid_from, 
				  --nvl(lag(lfgja||'0'||lpad(lfmon,2,'0'),1) over (partition by bwkey,matnr order by lfgja||'0'||lpad(lfmon,2,'0')),'1900001') as valid_from, 
				  nvl(lead(lfgja||'0'||lpad(lfmon,2,'0'),1) over (partition by bwkey,ltrim(matnr,'0') order by lfgja||'0'||lpad(lfmon,2,'0')),'2099012') as valid_to, 
				  peinh as unit,
				  stprs as std_price
from rg_itg.itg_ecc_standard_cost_history
),

emd as (select matl_num,matl_desc from rg_edw.edw_material_dim),
ecd as (select distinct cust_num,cust_nm from rg_edw.edw_customer_base_dim),
pac as ( select market_code,materialnumber,materialdescription,valid_from, 
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from 
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from, 
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                  pre_apsc_per_pc 
            from rg_itg.itg_mds_pre_apsc_master 
          --where -- materialnumber = '6127076' and     
            --market_code ='Korea'
         )  base 

        order by market_code, materialnumber, valid_from
       )

SELECT DISTINCT copa.fisc_yr,
       copa.fisc_yr_per AS period,
       copa.co_cd,
       copa.obj_crncy_co_obj AS currency,
       copa.sls_org AS plant,
       copa.prft_Ctr AS profit_cntr,
       copa.matl_num,
       emd.matl_desc,
       copa.cust_num,
       ecd.cust_nm,
       copa.nts,
       copa.nts_volume,
       copa.freegood_amt AS free_goods_value,
       stdc.std_price AS standard_cost,
       copa.freegood_amt*stdc.std_price AS freegoods_cost_per_unit,
       stdc.unit,
       case when sls_org = '320S' 
	        then standard_cost/unit*100
	        else standard_cost/unit end as Standard_cost_per_unit,
       pac.pre_apsc_cper_pc,
       copa.nts_volume*pac.pre_apsc_cper_pc AS COGS_at_Pre_APSC,
       ((CASE
         WHEN Standard_cost_per_unit = 0 THEN NULL
         ELSE (free_goods_value) / Standard_cost_per_unit
       END 
)*pac.pre_apsc_cper_pc) * -1 AS Free_Goods_COGS_at_Pre_APSC,
       COGS_at_Pre_APSC + Free_Goods_COGS_at_Pre_APSC AS Total_APSC
FROM copa
  LEFT JOIN stdc
         ON copa.matl_num = stdc.matnr
        AND copa.sls_org = stdc.bwkey
		AND copa.fisc_yr_per >= stdc.valid_from  
        AND copa.fisc_yr_per < stdc.valid_to 
  LEFT JOIN emd ON LTRIM (emd.matl_num,'0') = copa.matl_num
  LEFT JOIN ecd ON LTRIM (ecd.cust_num,'0') = copa.cust_num
  LEFT JOIN pac
         ON pac.materialnumber = copa.matl_num
        AND pac.market_code = copa.ctry_group
        AND copa.fisc_yr_per >= pac.valid_from  
        AND copa.fisc_yr_per < pac.valid_to 
);
        


--CREATE TABLE rg_edw.cogs_automation 
--AS
Insert into  rg_edw.edw_custgp_cogs_automation 
     (fisc_yr,
       period,
       co_cd,
       currency,
       plant,
       profit_cntr,
       matl_num,
       matl_desc,
       cust_num,
       cust_nm,
       nts,
       nts_volume,
       free_goods_value,
       standard_cost,
       freegoods_cost_per_unit,
       unit,
       standard_cost_per_unit,
       pre_apsc_cper_pc,
       cogs_at_pre_apsc,
       free_goods_cogs_at_pre_apsc,
       total_apsc )


WITH copa
AS
(
---Fetch COGS and Free Goods from COPA-----
SELECT DISTINCT fisc_yr,
       fisc_yr_per,
       cmp.ctry_group,
       cogs.co_cd,
       prft_Ctr,
       (CASE 
        WHEN sls_org IS NULL THEN 
          (CASE 
            WHEN cogs.co_cd = '4130' THEN '2100' 
            WHEN cogs.co_cd = '8266' THEN '320S' 
            WHEN cogs.co_cd = '4481' THEN '2210' 
            ELSE sls_org 
            END) 
        WHEN sls_org = '2400' AND LTRIM(matl_num,'0') NOT IN ('41812332','41802332') 
            THEN '2500' 
        ELSE sls_org 
        END),
       obj_crncy_co_obj,
       LTRIM(cust_num,'0') AS cust_num,
       LTRIM(matl_num,'0') AS matl_num,
       CASE
         WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(amt_obj_crncy)
         ELSE 0
       END AS nts,
       CASE
         WHEN cogs.acct_hier_shrt_desc = 'FG' THEN SUM(amt_obj_crncy)
         ELSE 0
       END AS freegood_amt,
       CASE
         WHEN cogs.acct_hier_shrt_desc = 'NTS' THEN SUM(round(qty))
         ELSE 0
       END AS nts_volume
FROM rg_edw.edw_copa_trans_fact cogs--,rg_itg.itg_custgp_cogs_fg_control fgctl
  LEFT JOIN rg_edw.edw_company_dim cmp ON cogs.co_cd = cmp.co_cd
INNER JOIN rg_itg.itg_custgp_cogs_fg_control fgctl on 
													   cogs.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and 	
													   cogs.co_cd = fgctl.co_cd and 
													   cogs.fisc_yr_per >= fgctl.valid_from and
													   cogs.fisc_yr_per < fgctl.valid_to and 
													   case when cogs.acct_hier_shrt_desc = 'FG' 
													        then ltrim(cogs.acct_num,'0') = fgctl.gl_acct_num 
															when cogs.acct_hier_shrt_desc = 'NTS' 
													        then '0' end = fgctl.gl_acct_num and
													   fgctl.active = 'Y'
													   
WHERE   cogs.fisc_yr::CHARACTER VARYING::TEXT = 2022 and cogs.acct_hier_shrt_desc in ('NTS','FG')
GROUP BY fisc_yr,
         fisc_yr_per,
         cogs.co_cd,
         cmp.ctry_group,
         prft_ctr,
         sls_org,
         obj_crncy_co_obj,
         LTRIM(cust_num,'0'),
         LTRIM(matl_num,'0'),
         cogs.acct_hier_shrt_desc
 
 ),
stdc
AS
(
--Fetch Standard_cost---
-- SELECT DISTINCT (CASE WHEN bwkey = '2400' AND LTRIM(matnr,'0') NOT IN ('41812332','41802332') THEN '2500' 
--                   ELSE bwkey END),
--        LTRIM(matnr,'0') AS matnr,
--        peinh AS unit,
--        lfgja AS curr_yr,
--        pprdl AS curr_fy_price_per,
--        pprdv AS curr_fy_prev_price_per,
--        stprs AS std_price,
--        vmstp AS std_price_prev
-- FROM rg_edw.edw_ecc_standard_cost

select case when market ='TH' then 'Thailand'
       when market ='HK' then 'Hongkong'
       when market ='KR' then 'Korea'
       when market ='TW' then 'Taiwan'
       when market ='VN' then 'Vietnam'
       when market ='SG' then 'Singapore'
       when market ='MY' then 'Malaysia'
      end as Ctry
      , Materialnumber
      , (StdCost/Qty) StdCostperUnit
  from rg_sdl.sdl_customerpl_stdcost_2022
  --where market = 'KR' --and materialnumber = '6127076'
  ),

emd as (select matl_num,matl_desc from rg_edw.edw_material_dim),
ecd as (select distinct cust_num,cust_nm from rg_edw.edw_customer_base_dim),
pac as ( select market_code,materialnumber,materialdescription,valid_from, 
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from 
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from, 
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                 pre_apsc_per_pc 
             rg_itg.itg_mds_pre_apsc_master 
          --where -- materialnumber = '6127076' and     
            --market_code ='Korea'
         )  base 

        order by market_code, materialnumber, valid_from
       )

SELECT DISTINCT copa.fisc_yr,
       copa.fisc_yr_per AS period,
       copa.co_cd,
       copa.obj_crncy_co_obj AS currency,
       copa.sls_org AS plant,
       copa.prft_Ctr AS profit_cntr,
       copa.matl_num,
       emd.matl_desc,
       copa.cust_num,
       ecd.cust_nm
       copa.nts,
       copa.nts_volume,
       copa.freegood_amt AS free_goods_value,
       stdc.StdCostperUnit AS standard_cost,
       copa.freegood_amt*stdc.StdCostperUnit AS freegoods_cost_per_unit,
       1 as unit,
       StdCostperUnit AS Standard_cost_per_unit,
       pac.pre_apsc_cper_pc,
       copa.nts_volume*pac.pre_apsc_cper_pc AS COGS_at_Pre_APSC,
       ((CASE
             WHEN Standard_cost_per_unit = 0 THEN NULL
             ELSE (free_goods_value) / Standard_cost_per_unit
         END 
            )*pac.pre_apsc_cper_pc) * -1 AS Free_Goods_COGS_at_Pre_APSC,
       COGS_at_Pre_APSC + Free_Goods_COGS_at_Pre_APSC AS Total_APSC
FROM copa
  LEFT JOIN stdc
         ON copa.matl_num = stdc.Materialnumber
        AND copa.ctry_group = stdc.Ctry
  LEFT JOIN emd ON LTRIM (emd.matl_num,'0') = copa.matl_num
  left ecd ON LTRIM (ecd.cust_num,'0') = copa.cust_num
  LEFT JOIN pac
         ON pac.materialnumber = copa.matl_num
        AND pac.market_code = copa.ctry_group
        AND copa.fisc_yr_per >= pac.valid_from  
        AND copa.fisc_yr_per < pac.valid_to 
        
;
