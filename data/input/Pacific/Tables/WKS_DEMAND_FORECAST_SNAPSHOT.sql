
 --================statement 1================ 
DROP TABLE IF EXISTS AU_WKS.WKS_DEMAND_FORECAST_SNAPSHOT;

 --================statement 2================ 
create table au_wks.wks_demand_forecast_snapshot as
select
  edfs.pac_source_type,
  edfs.pac_subsource_type,
  edfs.snap_shot_dt,
  edfs.snapshot_week_no,
  edfs.snapshot_mnth_week_no,
  edfs.snapshot_mnth_shrt,
  edfs.snapshot_year,
  edfs.final_version_indicator,
  edfs.jj_period,
  edfs.jj_week_no,
  edfs.jj_wk,
  edfs.jj_mnth,
  edfs.jj_mnth_shrt,
  edfs.jj_mnth_long,
  edfs.jj_qrtr,
  edfs.jj_year,
  edfs.jj_mnth_tot,
  ltrim(vmd.matl_id, 0) as matl_no,
  vmd.matl_desc,
  mstrcd.master_code,
  ltrim(vapcd.parent_id, 0) as parent_id,
  mstrcd.parent_matl_desc,
  vmd.mega_brnd_cd,
  vmd.mega_brnd_desc,
  vmd.brnd_cd,
  vmd.brnd_desc,
  vmd.base_prod_cd,
  vmd.base_prod_desc,
  vmd.variant_cd,
  vmd.variant_desc,
  vmd.fran_cd,
  vmd.fran_desc,
  vmd.grp_fran_cd,
  vmd.grp_fran_desc,
  vmd.matl_type_cd,
  vmd.matl_type_desc,
  vmd.prod_fran_cd,
  vmd.prod_fran_desc,
  vmd.prod_hier_cd,
  vmd.prod_hier_desc,
  vmd.prod_mjr_cd,
  vmd.prod_mjr_desc,
  vmd.prod_mnr_cd,
  vmd.prod_mnr_desc,
  vmd.mercia_plan,
  vmd.putup_cd,
  vmd.putup_desc,
  vmd.bar_cd,
  ltrim(vcd.cust_no, 0) as cust_no,
  vcd.cmp_id,
  vcd.ctry_key,
  vcd.country,
  vcd.state_cd,
  vcd.post_cd,
  vcd.cust_suburb,
  vcd.cust_nm,
  vcd.fcst_chnl,
  vcd.fcst_chnl_desc,
  vcd.sales_office_cd,
  vcd.sales_office_desc,
  vcd.sales_grp_cd,
  vcd.sales_grp_desc,
  vcd.curr_cd,
  edfs.actual_sales_qty,
  edfs.apo_tot_frcst,
  edfs.apo_base_frcst,
  edfs.apo_promo_frcst,
  edfs.px_tot_frcst,
  edfs.px_base_frcst,
  edfs.px_promo_frcst
from au_edw.edw_demand_forecast_snapshot as edfs, au_edw.vw_dmnd_frcst_customer_dim as vcd, au_edw.vw_material_dim as vmd, au_edw.vw_apo_parent_child_dim as vapcd, (
  select distinct
    master_code,
    parent_matl_desc
  from au_edw.vw_apo_parent_child_dim
  where
    cmp_id = 7470
  union all
  select distinct
    master_code,
    parent_matl_desc
  from au_edw.vw_apo_parent_child_dim
  where
    not master_code in (
      select distinct
        master_code
      from au_edw.vw_apo_parent_child_dim
      where
        cmp_id = 7470
    )
) as mstrcd
where
  edfs.pac_subsource_type <> 'sapbw_apo_forecast'
  and edfs.cust_no = ltrim(vcd.cust_no(), '0')
  and edfs.matl_no = ltrim(vmd.matl_id(), '0')
  and (
    edfs.cmp_id = vapcd.cmp_id() and edfs.matl_no = ltrim(vapcd.matl_id(), '0')
  )
  and vapcd.master_code = mstrcd.master_code();

 --================statement 3================ 
COMMIT;

 --================statement 4================ 
insert into au_wks.wks_demand_forecast_snapshot
select
  edfs.pac_source_type,
  edfs.pac_subsource_type,
  edfs.snap_shot_dt,
  edfs.snapshot_week_no,
  edfs.snapshot_mnth_week_no,
  edfs.snapshot_mnth_shrt,
  edfs.snapshot_year,
  edfs.final_version_indicator,
  edfs.jj_period,
  edfs.jj_week_no,
  edfs.jj_wk,
  edfs.jj_mnth,
  edfs.jj_mnth_shrt,
  edfs.jj_mnth_long,
  edfs.jj_qrtr,
  edfs.jj_year,
  edfs.jj_mnth_tot,
  ltrim(vmd.matl_id, 0) as matl_no,
  vmd.matl_desc,
  mstrcd.master_code,
  ltrim(vapcd.parent_id, 0) as parent_id,
  mstrcd.parent_matl_desc,
  vmd.mega_brnd_cd,
  vmd.mega_brnd_desc,
  vmd.brnd_cd,
  vmd.brnd_desc,
  vmd.base_prod_cd,
  vmd.base_prod_desc,
  vmd.variant_cd,
  vmd.variant_desc,
  vmd.fran_cd,
  vmd.fran_desc,
  vmd.grp_fran_cd,
  vmd.grp_fran_desc,
  vmd.matl_type_cd,
  vmd.matl_type_desc,
  vmd.prod_fran_cd,
  vmd.prod_fran_desc,
  vmd.prod_hier_cd,
  vmd.prod_hier_desc,
  vmd.prod_mjr_cd,
  vmd.prod_mjr_desc,
  vmd.prod_mnr_cd,
  vmd.prod_mnr_desc,
  vmd.mercia_plan,
  vmd.putup_cd,
  vmd.putup_desc,
  vmd.bar_cd,
  null as cust_no,
  vcd.cmp_id,
  null as ctry_key,
  vcd.country,
  null as state_cd,
  null as post_cd,
  null as cust_suburb,
  null as cust_nm,
  vcd.fcst_chnl,
  vcd.fcst_chnl_desc,
  null as sales_office_cd,
  null as sales_office_desc,
  null as sales_grp_cd,
  null as sales_grp_desc,
  null as curr_cd,
  edfs.actual_sales_qty,
  edfs.apo_tot_frcst,
  edfs.apo_base_frcst,
  edfs.apo_promo_frcst,
  edfs.px_tot_frcst,
  edfs.px_base_frcst,
  edfs.px_promo_frcst
from au_edw.edw_demand_forecast_snapshot as edfs, (
  select distinct
    cmp_id,
    country,
    sls_org,
    fcst_chnl,
    fcst_chnl_desc
  from au_edw.vw_dmnd_frcst_customer_dim
) as vcd, au_edw.vw_material_dim as vmd, au_edw.vw_apo_parent_child_dim as vapcd, (
  select distinct
    master_code,
    parent_matl_desc
  from au_edw.vw_apo_parent_child_dim
  where
    cmp_id = 7470
  union all
  select distinct
    master_code,
    parent_matl_desc
  from au_edw.vw_apo_parent_child_dim
  where
    not master_code in (
      select distinct
        master_code
      from au_edw.vw_apo_parent_child_dim
      where
        cmp_id = 7470
    )
) as mstrcd
where
  edfs.pac_subsource_type = 'sapbw_apo_forecast'
  and edfs.fcst_chnl = vcd.fcst_chnl()
  and edfs.matl_no = ltrim(vmd.matl_id(), '0')
  and (
    edfs.cmp_id = vapcd.cmp_id() and edfs.matl_no = ltrim(vapcd.matl_id(), '0')
  )
  and vapcd.master_code = mstrcd.master_code();

 --================statement 5================ 
COMMIT;

 --================statement 6================ 
TRUNCATE AU_EDW.EDW_DEMAND_FORECAST_SNAPSHOT;

 --================statement 7================ 
insert into au_edw.edw_demand_forecast_snapshot (
  pac_source_type,
  pac_subsource_type,
  snap_shot_dt,
  snapshot_week_no,
  snapshot_mnth_week_no,
  snapshot_mnth_shrt,
  snapshot_year,
  final_version_indicator,
  jj_period,
  jj_week_no,
  jj_wk,
  jj_mnth,
  jj_mnth_shrt,
  jj_mnth_long,
  jj_qrtr,
  jj_year,
  jj_mnth_tot,
  matl_no,
  matl_desc,
  master_code,
  parent_id,
  parent_matl_desc,
  mega_brnd_cd,
  mega_brnd_desc,
  brnd_cd,
  brnd_desc,
  base_prod_cd,
  base_prod_desc,
  variant_cd,
  variant_desc,
  fran_cd,
  fran_desc,
  grp_fran_cd,
  grp_fran_desc,
  matl_type_cd,
  matl_type_desc,
  prod_fran_cd,
  prod_fran_desc,
  prod_hier_cd,
  prod_hier_desc,
  prod_mjr_cd,
  prod_mjr_desc,
  prod_mnr_cd,
  prod_mnr_desc,
  mercia_plan,
  putup_cd,
  putup_desc,
  bar_cd,
  cust_no,
  cmp_id,
  ctry_key,
  country,
  state_cd,
  post_cd,
  cust_suburb,
  cust_nm,
  fcst_chnl,
  fcst_chnl_desc,
  sales_office_cd,
  sales_office_desc,
  sales_grp_cd,
  sales_grp_desc,
  curr_cd,
  actual_sales_qty,
  apo_tot_frcst,
  apo_base_frcst,
  apo_promo_frcst,
  px_tot_frcst,
  px_base_frcst,
  px_promo_frcst
)
select
  pac_source_type as pac_source_type,
  pac_subsource_type as pac_subsource_type,
  snap_shot_dt as snap_shot_dt,
  snapshot_week_no as snapshot_week_no,
  snapshot_mnth_week_no as snapshot_mnth_week_no,
  snapshot_mnth_shrt as snapshot_mnth_shrt,
  snapshot_year as snapshot_year,
  final_version_indicator as final_version_indicator,
  jj_period as jj_period,
  jj_week_no as jj_week_no,
  jj_wk as jj_wk,
  jj_mnth as jj_mnth,
  jj_mnth_shrt as jj_mnth_shrt,
  jj_mnth_long as jj_mnth_long,
  jj_qrtr as jj_qrtr,
  jj_year as jj_year,
  jj_mnth_tot as jj_mnth_tot,
  matl_no as matl_no,
  matl_desc as matl_desc,
  master_code as master_code,
  parent_id as parent_id,
  parent_matl_desc as parent_matl_desc,
  mega_brnd_cd as mega_brnd_cd,
  mega_brnd_desc as mega_brnd_desc,
  brnd_cd as brnd_cd,
  brnd_desc as brnd_desc,
  base_prod_cd as base_prod_cd,
  base_prod_desc as base_prod_desc,
  variant_cd as variant_cd,
  variant_desc as variant_desc,
  fran_cd as fran_cd,
  fran_desc as fran_desc,
  grp_fran_cd as grp_fran_cd,
  grp_fran_desc as grp_fran_desc,
  matl_type_cd as matl_type_cd,
  matl_type_desc as matl_type_desc,
  prod_fran_cd as prod_fran_cd,
  prod_fran_desc as prod_fran_desc,
  prod_hier_cd as prod_hier_cd,
  prod_hier_desc as prod_hier_desc,
  prod_mjr_cd as prod_mjr_cd,
  prod_mjr_desc as prod_mjr_desc,
  prod_mnr_cd as prod_mnr_cd,
  prod_mnr_desc as prod_mnr_desc,
  mercia_plan as mercia_plan,
  putup_cd as putup_cd,
  putup_desc as putup_desc,
  bar_cd as bar_cd,
  cust_no as cust_no,
  cmp_id as cmp_id,
  ctry_key as ctry_key,
  country as country,
  state_cd as state_cd,
  post_cd as post_cd,
  cust_suburb as cust_suburb,
  cust_nm as cust_nm,
  fcst_chnl as fcst_chnl,
  fcst_chnl_desc as fcst_chnl_desc,
  sales_office_cd as sales_office_cd,
  sales_office_desc as sales_office_desc,
  sales_grp_cd as sales_grp_cd,
  sales_grp_desc as sales_grp_desc,
  curr_cd as curr_cd,
  actual_sales_qty as actual_sales_qty,
  apo_tot_frcst as apo_tot_frcst,
  apo_base_frcst as apo_base_frcst,
  apo_promo_frcst as apo_promo_frcst,
  px_tot_frcst as px_tot_frcst,
  px_base_frcst as px_base_frcst,
  px_promo_frcst as px_promo_frcst
from au_wks.wks_demand_forecast_snapshot;
