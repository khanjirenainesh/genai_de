
 --================statement 1================ 
truncate au_edw.edw_apo_weekly_forecast_fact;

 --================statement 2================ 
commit;

 --================statement 3================ 
insert into au_edw.edw_apo_weekly_forecast_fact
select
  salesorg,
  mercia_reference,
  material,
  substring(fiscper, 1, 4) || substring(fiscper, 6, 7),
  calweek,
  to_char(cast(jnj_fiscal_week as timestampntz), 'yyyymmDD'),
  tot_forecast,
  tot_bas_fct,
  tot_forecast - tot_bas_fct
from rg_itg.itg_weekly_forecast
where
  to_char(cast(snap_dt as timestampntz), 'yyyymmDD') = (
    select
      to_char(cast(max(snap_dt) as timestampntz), 'yyyymmDD')
    from rg_itg.itg_weekly_forecast
    where
      fiscyear = (
        select
          jj_year
        from au_edw.edw_time_dim
        where
          trunc(cal_date) = trunc(current_timestamp())
      )
  )
  and salesorg in ('3300', '3410');

 --================statement 4================ 
commit;
