
truncate au_sdl.sdl_px_term_actual;

commit;

copy au_sdl.sdl_px_term_actual
(
ac_code,  
ac_attribute,  
ac_longname,  
sku_stockcode,  
sku_attribute,  
sku_longname,  
gltt_longname,  
bd_shortname,  
bd_longname,  
asas_rowid,  
asas_pubid,  
asas_type,  
asas_month1,  
asas_month2,  
asas_month3,  
asas_month4,  
asas_month5,  
asas_month6,  
asas_month7,  
asas_month8,  
asas_month9,  
asas_month10,  
asas_month11,  
asas_month12 ,
gltt_rowid
) 

from 's3://+context.s3_cdl_bucket_px+/px_term_actual+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
delimiter ';';


commit;



truncate au_itg.itg_px_term_actual;
commit;

insert into au_itg.itg_px_term_actual
select * from au_sdl.sdl_px_term_actual;

commit;



truncate au_edw.edw_px_term_actual;
commit;

insert into au_edw.edw_px_term_actual
                                                ([ac_code]
           ,[ac_attribute]
           ,[ac_longname]
           ,[sku_stockcode]
           ,[sku_attribute]
           ,[sku_longname]
           ,[gltt_longname]
           ,[bd_shortname]
           ,[bd_longname]
           ,[asas_rowid]
           ,[asas_pubid]
           ,[asas_type]
           ,[asas_month1]
           ,[asas_month2]
           ,[asas_month3]
           ,[asas_month4]
           ,[asas_month5]
           ,[asas_month6]
           ,[asas_month7]
           ,[asas_month8]
           ,[asas_month9]
           ,[asas_month10]
           ,[asas_month11]
           ,[asas_month12]
           ,[gltt_rowid])
     select [ac_code]
           ,[ac_attribute]
           ,[ac_longname]
           ,[sku_stockcode]
           ,[sku_attribute]
           ,[sku_longname]
           ,[gltt_longname]
           ,substring([bd_shortname],6,4)
           ,[bd_longname]
           ,[asas_rowid]
           ,[asas_pubid]
           ,[asas_type]
           ,[asas_month1]
           ,[asas_month2]
           ,[asas_month3]
           ,[asas_month4]
           ,[asas_month5]
           ,[asas_month6]
           ,[asas_month7]
           ,[asas_month8]
           ,[asas_month9]
           ,[asas_month10]
           ,[asas_month11]
           ,[asas_month12]
           ,[gltt_rowid]
     from au_itg.itg_px_term_actual;

commit;



truncate au_edw.edw_px_term_actual_ext;
commit;

insert into au_edw.edw_px_term_actual_ext
                                                (cmp_id, channel_id, cust_id, matl_id, gltt_longname, time_period,px_term_actual_amt,year, gltt_rowid)
                select  '0000','00',
                                                ac_attribute,sku_stockcode, gltt_longname, timeperiod,
                                                case timeperiod
                                                                when 1 then [asas_month1]
                                                                when 2 then [asas_month2]
                                                                when 3 then [asas_month3]
                                                                when 4 then [asas_month4]
                                                                when 5 then [asas_month5]
                                                                when 6 then [asas_month6]
                                                                when 7 then [asas_month7]
                                                                when 8 then [asas_month8]
                                                                when 9 then [asas_month9]
                                                                when 10 then [asas_month10]
                                                                when 11 then [asas_month11]
                                                                when 12 then [asas_month12]
                                                end       as px_term_actual_amt, bd_shortname, gltt_rowid
                from (select edw_px_term_actual.ac_code, edw_px_term_actual.ac_attribute, edw_px_term_actual.ac_longname, edw_px_term_actual.sku_stockcode, edw_px_term_actual.sku_attribute, edw_px_term_actual.sku_longname, edw_px_term_actual.gltt_longname, edw_px_term_actual.bd_shortname, edw_px_term_actual.bd_longname, edw_px_term_actual.asas_type, sum(edw_px_term_actual.asas_month1) as asas_month1, sum(edw_px_term_actual.asas_month2) as asas_month2, sum(edw_px_term_actual.asas_month3) as asas_month3, sum(edw_px_term_actual.asas_month4) as asas_month4, sum(edw_px_term_actual.asas_month5) as asas_month5, sum(edw_px_term_actual.asas_month6) as asas_month6, sum(edw_px_term_actual.asas_month7) as asas_month7, sum(edw_px_term_actual.asas_month8) as asas_month8, sum(edw_px_term_actual.asas_month9) as asas_month9, sum(edw_px_term_actual.asas_month10) as asas_month10, sum(edw_px_term_actual.asas_month11) as asas_month11, sum(edw_px_term_actual.asas_month12) as asas_month12, edw_px_term_actual.gltt_rowid
   from au_edw.edw_px_term_actual
  group by edw_px_term_actual.ac_code, edw_px_term_actual.ac_attribute, edw_px_term_actual.ac_longname, edw_px_term_actual.sku_stockcode, edw_px_term_actual.sku_attribute, edw_px_term_actual.sku_longname, edw_px_term_actual.gltt_longname, edw_px_term_actual.bd_shortname, edw_px_term_actual.bd_longname, edw_px_term_actual.asas_type, edw_px_term_actual.gltt_rowid),
                                                (select 1 as timeperiod
                                                  union all select 2
                                                  union all select 3
                                                  union all select 4
                                                  union all select 5
                                                  union all select 6
                                                  union all select 7
                                                  union all select 8
                                                  union all select 9
                                                  union all select 10
                                                  union all select 11
                                                  union all select 12) as m where ac_attribute <> '  ';
                                                  
commit;



--1

update au_edw.edw_px_term_actual_ext
                set time_period = case time_period
                                            when '1' then convert(numeric,(year+'01'))
                                            when '2' then convert(numeric,(year+'02'))
                                            when '3' then convert(numeric,(year+'03'))
                                            when '4' then convert(numeric,(year+'04'))
                                            when '5' then convert(numeric,(year+'05'))
                                            when '6' then convert(numeric,(year+'06'))
                                            when '7' then convert(numeric,(year+'07'))
                                            when '8' then convert(numeric,(year+'08'))
                                            when '9' then convert(numeric,(year+'09'))
                                            when '10' then convert(numeric,(year+'10'))
                                            when '11' then convert(numeric,(year+'11'))
                                            when '12' then convert(numeric,(year+'12'))
                                  end ;
                                  
commit;

--2

update au_edw.edw_px_term_actual_ext
set cmp_id=b.cmp_id
from (select distinct cust_no,vcd.cmp_id from au_edw.vw_customer_dim vcd,au_edw.edw_px_term_actual_ext eptae 
where eptae.cust_id=substring(cust_no,5,6))b
where cust_id =substring(b.cust_no,5,6) ;

commit;

--3

update au_edw.edw_px_term_actual_ext
set channel_id=b.channel_cd
from (select distinct cust_no,vcd.channel_cd from au_edw.vw_customer_dim vcd,au_edw.edw_px_term_actual_ext eptae 
where eptae.cust_id=substring(cust_no,5,6))b
where cust_id =substring(b.cust_no,5,6); 

commit;

--4

update au_edw.edw_px_term_actual_ext set channel_id = '10 - au' where channel_id = '10' and cmp_id = '7470';
update au_edw.edw_px_term_actual_ext set channel_id = '11 - au' where channel_id = '11' and cmp_id = '7470';
update au_edw.edw_px_term_actual_ext set channel_id = '15 - au' where channel_id = '15' and cmp_id = '7470';
update au_edw.edw_px_term_actual_ext set channel_id = '19 - au' where channel_id = '19' and cmp_id = '7470';
update au_edw.edw_px_term_actual_ext set channel_id = '10 - nz' where channel_id = '10' and cmp_id = '8361';
update au_edw.edw_px_term_actual_ext set channel_id = '11 - nz' where channel_id = '11' and cmp_id = '8361';

commit;




truncate au_sdl.sdl_px_term_plan;

commit;

copy au_sdl.sdl_px_term_plan
(
ac_code,  
ac_attribute,
ac_longname,
sku_stockcode,
sku_attribute,
sku_longname,
gltt_longname,
bd_shortname,
bd_longname,
asps_rowid,
asps_pubid,
asps_type,
asps_month1,
asps_month2,
asps_month3,
asps_month4,
asps_month5,
asps_month6,
asps_month7,
asps_month8,
asps_month9,
asps_month10,
asps_month11,
asps_month12,
gltt_rowid
) 

from 's3://+context.s3_cdl_bucket_px+/px_term_plan+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
delimiter ';';


commit;



truncate au_itg.itg_px_term_plan;
commit;

insert into au_itg.itg_px_term_plan
select * from au_sdl.sdl_px_term_plan;

commit;
ac_code::varchar(50) as ac_code,
ac_attribute::varchar(20) as ac_attribute,
ac_longname::varchar(40) as ac_longname,
sku_stockcode::varchar(18) as sku_stockcode,
sku_attribute::varchar(20) as sku_attribute,
sku_longname::varchar(40) as sku_longname,
gltt_longname::varchar(40) as gltt_longname,
bd_shortname::varchar(10) as bd_shortname,
bd_longname::varchar(40) as bd_longname,
asps_rowid::number(18,0) as asps_rowid,
asps_pubid::varchar(30) as asps_pubid,
asps_type::number(38,0) as asps_type,
asps_month1::float as asps_month1,
asps_month2::float as asps_month2,
asps_month3::float as asps_month3,
asps_month4::float as asps_month4,
asps_month5::float as asps_month5,
asps_month6::float as asps_month6,
asps_month7::float as asps_month7,
asps_month8::float as asps_month8,
asps_month9::float as asps_month9,
asps_month10::float as asps_month10,
asps_month11::float as asps_month11,
asps_month12::float as asps_month12,
gltt_rowid::number(18,0) as gltt_rowid,




truncate au_edw.edw_px_term_plan;
commit;

insert  into au_edw.edw_px_term_plan
                                                ([ac_code]
           ,[ac_attribute]
           ,[ac_longname]
           ,[sku_stockcode]
           ,[sku_attribute]
           ,[sku_longname]
           ,[gltt_longname]
           ,[bd_shortname]
           ,[bd_longname]
           ,[asps_rowid]
                                   ,[asps_pubid]
                                   ,[asps_type]
           ,[asps_month1]
           ,[asps_month2]
           ,[asps_month3]
           ,[asps_month4]
           ,[asps_month5]
           ,[asps_month6]
           ,[asps_month7]
           ,[asps_month8]
           ,[asps_month9]
           ,[asps_month10]
           ,[asps_month11]
           ,[asps_month12]
           ,[gltt_rowid]) 
select [ac_code]
           ,[ac_attribute]
           ,[ac_longname]
           ,[sku_stockcode]
           ,[sku_attribute]
           ,[sku_longname]
           ,[gltt_longname]
           ,substring([bd_shortname],6,4)
           ,[bd_longname]
           ,[asps_rowid]
                                   ,'0'
           ,[asps_type]
           ,[asps_month1]
           ,[asps_month2]
           ,[asps_month3]
           ,[asps_month4]
           ,[asps_month5]
           ,[asps_month6]
           ,[asps_month7]
           ,[asps_month8]
           ,[asps_month9]
           ,[asps_month10]
           ,[asps_month11]
           ,[asps_month12]
           ,[gltt_rowid]
     from au_itg.itg_px_term_plan;

commit;

 ac_code::varchar(50) as ac_code,
 ac_attribute::varchar(20) as ac_attribute,
 ac_longname::varchar(40) as ac_longname,
 sku_stockcode::varchar(18) as sku_stockcode,
 sku_attribute::varchar(20) as sku_attribute,
 sku_longname::varchar(40) as sku_longname,
 gltt_longname::varchar(40) as gltt_longname,
 substring([bd_shortname],6,4)::varchar(10) as bd_shortname,
 bd_longname::varchar(40) as bd_longname,
 asps_rowid::number(18,0) as asps_rowid,
 '0'::varchar(30) as asps_pubid,
 asps_type::number(38,0) as asps_type,
 asps_month1::float as asps_month1,
 asps_month2::float as asps_month2,
 asps_month3::float as asps_month3,
 asps_month4::float as asps_month4,
 asps_month5::float as asps_month5,
 asps_month6::float as asps_month6,
 asps_month7::float as asps_month7,
 asps_month8::float as asps_month8,
 asps_month9::float as asps_month9,
 asps_month10::float as asps_month10,
 asps_month11::float as asps_month11,
 asps_month12::float as asps_month12,
 gltt_rowid::number(18,0) as gltt_rowid,


truncate au_edw.edw_px_term_plan_ext;
commit;


insert into au_edw.edw_px_term_plan_ext
                                                (cmp_id, channel_id, cust_id, matl_id, gltt_longname, time_period,px_term_proj_amt,year, gltt_rowid)
                select  '0000','00',
                                                ac_attribute,sku_stockcode, gltt_longname, timeperiod,
                                                case timeperiod
                                                                when 1 then [asps_month1]
                                                                when 2 then [asps_month2]
                                                                when 3 then [asps_month3]
                                                                when 4 then [asps_month4]
                                                                when 5 then [asps_month5]
                                                                when 6 then [asps_month6]
                                                                when 7 then [asps_month7]
                                                                when 8 then [asps_month8]
                                                                when 9 then [asps_month9]
                                                                when 10 then [asps_month10]
                                                                when 11 then [asps_month11]
                                                                when 12 then [asps_month12]
                                                end       as px_term_proj_amt, bd_shortname, gltt_rowid
                from (select edw_px_term_plan.ac_code, edw_px_term_plan.ac_attribute, edw_px_term_plan.ac_longname, edw_px_term_plan.sku_stockcode, edw_px_term_plan.sku_attribute, edw_px_term_plan.sku_longname, edw_px_term_plan.gltt_longname, edw_px_term_plan.bd_shortname, edw_px_term_plan.bd_longname, edw_px_term_plan.asps_type, sum(edw_px_term_plan.asps_month1) as asps_month1, sum(edw_px_term_plan.asps_month2) as asps_month2, sum(edw_px_term_plan.asps_month3) as asps_month3, sum(edw_px_term_plan.asps_month4) as asps_month4, sum(edw_px_term_plan.asps_month5) as asps_month5, sum(edw_px_term_plan.asps_month6) as asps_month6, sum(edw_px_term_plan.asps_month7) as asps_month7, sum(edw_px_term_plan.asps_month8) as asps_month8, sum(edw_px_term_plan.asps_month9) as asps_month9, sum(edw_px_term_plan.asps_month10) as asps_month10, sum(edw_px_term_plan.asps_month11) as asps_month11, sum(edw_px_term_plan.asps_month12) as asps_month12, edw_px_term_plan.gltt_rowid
   from au_edw.edw_px_term_plan
  group by edw_px_term_plan.ac_code, edw_px_term_plan.ac_attribute, edw_px_term_plan.ac_longname, edw_px_term_plan.sku_stockcode, edw_px_term_plan.sku_attribute, edw_px_term_plan.sku_longname, edw_px_term_plan.gltt_longname, edw_px_term_plan.bd_shortname, edw_px_term_plan.bd_longname, edw_px_term_plan.asps_type, edw_px_term_plan.gltt_rowid),
                                                (select 1 as timeperiod
                                                  union all select 2
                                                  union all select 3
                                                  union all select 4
                                                  union all select 5
                                                  union all select 6
                                                  union all select 7
                                                  union all select 8
                                                  union all select 9
                                                  union all select 10
                                                  union all select 11
                                                  union all select 12) as m where ac_attribute <> '  ';

commit;



--1

update au_edw.edw_px_term_plan_ext
                set time_period = case time_period
                                           when '1' then convert(numeric,(year+'01'))
                                            when '2' then convert(numeric,(year+'02'))
                                            when '3' then convert(numeric,(year+'03'))
                                            when '4' then convert(numeric,(year+'04'))
                                            when '5' then convert(numeric,(year+'05'))
                                            when '6' then convert(numeric,(year+'06'))
                                            when '7' then convert(numeric,(year+'07'))
                                            when '8' then convert(numeric,(year+'08'))
                                            when '9' then convert(numeric,(year+'09'))
                                            when '10' then convert(numeric,(year+'10'))
                                            when '11' then convert(numeric,(year+'11'))
                                            when '12' then convert(numeric,(year+'12'))
                                  end ;
                                  
commit;



--2

update au_edw.edw_px_term_plan_ext
set cmp_id=b.cmp_id
from (select distinct cust_no,vcd.cmp_id from au_edw.vw_customer_dim vcd,au_edw.edw_px_term_plan_ext eptae where eptae.cust_id=substring(cust_no,5,6))b
where cust_id =substring(b.cust_no,5,6);

commit;

--3

update au_edw.edw_px_term_plan_ext
set channel_id=b.channel_cd
from (select distinct cust_no,vcd.channel_cd from au_edw.vw_customer_dim vcd,au_edw.edw_px_term_plan_ext eptae where eptae.cust_id=substring(cust_no,5,6))b
where cust_id =substring(b.cust_no,5,6); 

commit;

--4

update au_edw.edw_px_term_plan_ext set channel_id = '10 - au' where channel_id = '10' and cmp_id = '7470';
update au_edw.edw_px_term_plan_ext set channel_id = '11 - au' where channel_id = '11' and cmp_id = '7470';
update au_edw.edw_px_term_plan_ext set channel_id = '15 - au' where channel_id = '15' and cmp_id = '7470';
update au_edw.edw_px_term_plan_ext set channel_id = '19 - au' where channel_id = '19' and cmp_id = '7470';
update au_edw.edw_px_term_plan_ext set channel_id = '10 - nz' where channel_id = '10' and cmp_id = '8361';
update au_edw.edw_px_term_plan_ext set channel_id = '11 - nz' where channel_id = '11' and cmp_id = '8361';

commit;



ITG_PX_APN
EDW_PX_APN
ITG_PX_LISTPRICE
EDW_PX_LISTPRICE
ITG_PX_FORECAST
EDW_PX_FORECAST_FACT
ITG_PX_WEEKLY_SELL
ITG_PX_MASTER
EDW_PX_MASTER_FACT

ITG_PX_SCAN_DATA
EDW_px_scan_data_fact
ITG_PX_UOM
EDW_PX_UOM
ITG_PX_TERM_PLAN
EDW_PX_TERM_PLAN

truncate au_sdl.sdl_px_apn;

commit;

copy au_sdl.sdl_px_apn
(
sku_apncode,   
apn,            
sku_stockcode
) 

from 's3://+context.s3_cdl_bucket_px+/px_apn+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
delimiter ';';


commit;



truncate au_itg.itg_px_apn;

commit;

insert into au_itg.itg_px_apn
select * from au_sdl.sdl_px_apn;


commit;



truncate au_edw.edw_px_apn;
commit;

insert into  au_edw.edw_px_apn
select * from au_itg.itg_px_apn


commit;


	
truncate au_sdl.sdl_px_forecast;

commit;

copy au_sdl.sdl_px_forecast
(
ac_code,
ac_attribute,
sku_stockcode,
sku_attribute,
sku_profitcentre,
est_date,
est_normal,
est_promotional,
est_estimate
) 

from 's3://+context.s3_cdl_bucket_px+/px_forecast+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
dateformat 'dd-mm-yyyy'
delimiter ';';


commit;

	select min(est_date), max(est_date) from au_sdl.sdl_px_forecast;

delete from au_itg.itg_px_forecast 
where est_date >= dateadd(month, -42, getdate());
delete from au_itg.itg_px_forecast where est_date >= DATEADD('month', -42, CURRENT_DATE())
commit;


	

insert into au_itg.itg_px_forecast
select * from au_sdl.sdl_px_forecast;

commit;



truncate au_edw.edw_px_forecast_fact;

commit;

insert into au_edw.edw_px_forecast_fact
select * from au_itg.itg_px_forecast
where (est_normal <> 0) or (est_promotional <> 0) or (est_estimate <> 0);

commit;



	
	
truncate au_sdl.sdl_px_listprice;

commit;

copy au_sdl.sdl_px_listprice
(
sku_stockcode, 
lp_price,
lp_cost,
lp_salestax,
lp_startdate,
lp_stopdate,
sku_uompersaleable,
sales_org
) 

from 's3://+context.s3_cdl_bucket_px+/px_listprice+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
dateformat 'yyyy-mm-dd'
delimiter ';';


commit;


truncate au_itg.itg_px_listprice;

commit;

insert into au_itg.itg_px_listprice
select * from au_sdl.sdl_px_listprice;

commit;


	
truncate au_edw.edw_px_listprice;

commit;

insert into au_edw.edw_px_listprice
select * from au_itg.itg_px_listprice;

commit;


	
update au_edw.edw_px_listprice set lp_stopdate = getdate()+1800 where lp_stopdate is null;
commit;

truncate au_sdl.sdl_px_master;

commit;

copy au_sdl.sdl_px_master
(
ac_code,
ac_longname,
ac_attribute,
p_promonumber,
p_startdate,
p_stopdate,
promo_length,
p_buystartdatedef,
p_buystopdatedef,
buyperiod_length,
hierarchy_rowid,
hierarchy_longname,
activity_longname,
confirmed_switch,
closed_switch,
sku_longname,
sku_stockcode,
sku_profitcentre,
sku_attribute,
gltt_rowid,
transaction_longname,
case_deal,
case_quantity,
planspend_total,
paid_total,
p_deleted,
--grp_shortname,
--grp_longname,
transaction_attribute,
promotionrowid,
normal_qty
) 

from 's3://+context.s3_cdl_bucket_px+/stg_px_master+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
dateformat 'yyyy-mm-dd'
delimiter ';';


commit;


	select min(p_startdate), max(p_startdate) from au_itg.itg_px_master;


delete from au_itg.itg_px_master 
where p_startdate >= convert(datetime,dateadd(month, -42, getdate()));

commit;


	

insert into au_itg.itg_px_master
select * from au_sdl.sdl_px_master;
 

commit;


	
truncate au_edw.edw_px_master_fact;
commit;

insert into au_edw.edw_px_master_fact                                                              
     (ac_code, ac_longname, cust_id, p_promonumber, p_startdate, p_stopdate, promo_length, p_buystartdatedef, p_buystopdatedef,                                                                        
     buyperiod_length, hierarchy_rowid, hierarchy_longname, activity_longname, confirmed_switch, closed_switch, sku_longname, matl_id,                                                                            
     sku_profitcentre, sku_attribute, gltt_rowid, transaction_longname, case_deal, case_quantity, planspend_total, paid_total, p_deleted,                                                                         
           --     grp_shortname, grp_longname,
 transaction_attribute,promotionforecastweek,promotionrowid,committed_spend)                                                                
    select                                                                             
      a.ac_code, a.ac_longname, a.ac_attribute, a.p_promonumber, a.p_startdate, a.p_stopdate, a.promo_length, a.p_buystartdatedef, a.p_buystopdatedef,                                                                        
     a.buyperiod_length, a.hierarchy_rowid, a.hierarchy_longname, a.activity_longname, a.confirmed_switch, a.closed_switch, a.sku_longname, a.sku_stockcode,                                                                                
     a.sku_profitcentre, a.sku_attribute, a.gltt_rowid, a.transaction_longname, a.case_deal, cast(b.case_quantity as integer), b.planspend_total, b.paid_total, a.p_deleted,                                                                         
--     a.grp_shortname, a.grp_longname,
 a.transaction_attribute, b.promotionforecastweek,  a.promotionrowid,
     (case when b.promotionitemstatus='closed' then  b.paid_total else (case when b.planspend_total  > b.paid_total then b.planspend_total else b.paid_total end) end) as committed_spend	 

     from au_itg.itg_px_master a join au_itg.itg_px_weekly_sell b on 
	 
	coalesce(a.promotionrowid,'-999999') = coalesce(b.promotionrowid,'-999999')
	and coalesce(a.gltt_rowid,'-999999')=coalesce(b.gltt_rowid,'-999999')
	and coalesce(a.sku_stockcode,'-999999')=coalesce(b.sku_stockcode,'-999999');


commit;


	
--1
update au_edw.edw_px_master_fact                                                                          
                set period = (select jj_mnth_id from au_edw.edw_time_dim where promotionforecastweek=cal_date); 

commit;

--2
update au_edw.edw_px_master_fact                                                        
set local_ccy = b.curr_cd
from
(select distinct cust_no,curr_cd from au_edw.vw_customer_dim vcd,au_edw.edw_px_master_fact epmf where epmf.cust_id = substring(cust_no,5,6)) b
where cust_id = substring(b.cust_no,5,6);

commit;

update au_edw.edw_px_master_fact set aud_rate = 1.0 where local_ccy = 'aud';  

commit;

--3

update au_edw.edw_px_master_fact                                                                
set aud_rate = fnl.exch_rate
from
(select distinct exch_rate,cal_date,jj_mnth_id,from_ccy,to_ccy,rate_type from au_edw.vw_curr_exch_dim a inner join
(select to_number(to_char(cal_date,'yyyymmdd'),'99999999') as cal_date,jj_mnth_id from au_edw.edw_time_dim ) b 
on a.valid_date=b.cal_date,au_edw.edw_px_master_fact c where b.jj_mnth_id = c.period 
and a.from_ccy = 'nzd' and a.to_ccy = 'aud' and a.rate_type = 'jjbr')fnl
where local_ccy = 'nzd'  and fnl.jj_mnth_id=au_edw.edw_px_master_fact.period; 

commit;

--4

update au_edw.edw_px_master_fact
   set aud_rate = (select a.exch_rate
                   from au_edw.vw_curr_exch_dim a
                   where a.from_ccy = 'nzd'
                   and   a.to_ccy = 'aud'
                   and   a.rate_type = 'jjbr'
                   and   valid_date = (select max(valid_date)
                                       from au_edw.vw_curr_exch_dim b
                                       where from_ccy = 'nzd'
                                       and   b.to_ccy = 'aud'
                                       and   b.rate_type = 'jjbr'))
where (local_ccy = 'nzd')
and   aud_rate is null;

commit;

--5

update au_edw.edw_px_master_fact
   set usd_rate = (select a.exch_rate
                   from au_edw.vw_curr_exch_dim a
                   where a.from_ccy = 'aud'
                   and   a.to_ccy = 'usd'
                   and   a.rate_type = 'dwbp'
                   and   valid_date = (select max(valid_date)
                                       from au_edw.vw_curr_exch_dim b
                                       where from_ccy = 'aud'
                                       and   b.to_ccy = 'usd'
                                       and   b.rate_type = 'dwbp'))
where (local_ccy = 'aud')
and   usd_rate is null;

commit;

--6

update au_edw.edw_px_master_fact
   set usd_rate = (select a.exch_rate
                   from au_edw.vw_curr_exch_dim a
                   where a.from_ccy = 'nzd'
                   and   a.to_ccy = 'usd'
                   and   a.rate_type = 'dwbp'
                   and   valid_date = (select max(valid_date)
                                       from au_edw.vw_curr_exch_dim b
                                       where from_ccy = 'nzd'
                                       and   b.to_ccy = 'usd'
                                       and   b.rate_type = 'dwbp'))
where (local_ccy = 'nzd')
and   usd_rate is null;

commit;

--7

update au_edw.edw_px_master_fact
                set transaction_attribute = 'tp'
                where transaction_attribute = '    ';
commit; 


	
truncate au_sdl.sdl_px_weekly_sell;

commit;

copy au_sdl.sdl_px_weekly_sell
(
promotionrowid	,
p_promonumber	,
transactionlongname	,
gltt_rowid	,
ac_longname	,
ac_code	,
activity	,
sku_stockcode	,
sku_longname	,
sku_profitcentre	,
promotionforecastweek	,
committed_amount,
planspend_total	,
paid_total	,
writeback_tot	,
balance_tot	,
case_quantity	,
promotionitemstatus	
) 

from 's3://+context.s3_cdl_bucket_px+/promotionweeklysell+talenddate.getdate(ccyy-mm-dd)+.csv.gz'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreheader 1  
dateformat 'auto' 
timeformat 'auto' 
acceptinvchars
truncatecolumns
ignoreblanklines 
blanksasnull 
emptyasnull
delimiter '|'
gzip ;


commit;


	

truncate au_itg.itg_px_weekly_sell;
commit;

insert into au_itg.itg_px_weekly_sell 
   (promotionrowid,
   p_promonumber,
   transactionlongname,
   gltt_rowid,
   ac_longname,
   ac_code,
   activity,
   sku_stockcode,
   sku_longname,
   sku_profitcentre,
   promotionforecastweek,
   planspend_total,
   paid_total,
   writeback_tot,
   balance_tot,
   case_quantity,
   promotionitemstatus)
select  cast(t1.promotionrowid as integer),
   cast(p_promonumber as varchar(10)),
   cast(transactionlongname as varchar(40)),
   cast(t1.gltt_rowid as float8),
   cast(ac_longname as varchar(40)),
   cast(ac_code as varchar(20)),
   cast(activity as varchar(40)),
   cast(t1.sku_stockcode as varchar(40)),
   cast(sku_longname as varchar(40)),
   cast(sku_profitcentre as varchar(40)),
   convert(datetime,convert(varchar(40),promotionforecastweek)),
   sum(cast(planspend_total as float8) ) planspend_total,
case when sum(cast(t2.paid_total as numeric(21,2)))=sum(cast(t2.committed_amount as numeric(21,2))) then sum(cast(t1.committed_amount as float8)) 
when sum(cast(t2.paid_total as numeric(21,2)))!=sum(cast(t2.committed_amount as numeric(21,2))) 
and (max(t2.committed_amount_max)=min(t2.committed_amount_min)) then sum(cast(t2.paid_total_e as float8))
when sum(cast(t2.paid_total as numeric(21,2)))!=sum(cast(t2.committed_amount as numeric(21,2))) 
and (max(t2.committed_amount_max)!=min(t2.committed_amount_min)) then 
sum(t2.paid_total)*(sum(cast(t1.committed_amount as float8))/sum(t2.committed_amount))
end as paid_total,
   sum(cast(writeback_tot as float8)) writeback_tot,
   sum(cast(balance_tot as float8))balance_tot,
   sum(cast(case_quantity as float8)) case_quantity,
   cast(t3.promotionitemstatus as varchar(40))
   from  au_sdl.sdl_px_weekly_sell t1 left join (select distinct promotionrowid,gltt_rowid,sku_stockcode,max(cast(committed_amount as float8)) committed_amount_max,
min(cast(committed_amount as float8)) committed_amount_min,
sum(cast(committed_amount as float8)) committed_amount,
   sum(cast(paid_total as float8)) paid_total,
   sum(cast(paid_total as float8))/count(sku_stockcode) as paid_total_e
    from au_sdl.sdl_px_weekly_sell 
    group by promotionrowid,gltt_rowid,sku_stockcode) t2 --tranformation1
   on coalesce(t1.promotionrowid,'-999999') = coalesce(t2.promotionrowid,'-999999')
	and coalesce(t1.gltt_rowid,'-999999')=coalesce(t2.gltt_rowid,'-999999')
	and coalesce(t1.sku_stockcode,'-999999')=coalesce(t2.sku_stockcode,'-999999')
	left join (select distinct promotionrowid,gltt_rowid,sku_stockcode, promotionitemstatus from au_sdl.sdl_px_weekly_sell where promotionitemstatus is) t3
	on coalesce(t1.promotionrowid,'-999999') = coalesce(t3.promotionrowid,'-999999')
	and coalesce(t1.gltt_rowid,'-999999')=coalesce(t3.gltt_rowid,'-999999')
	and coalesce(t1.sku_stockcode,'-999999')=coalesce(t3.sku_stockcode,'-999999')
  group by 	
  t1.promotionrowid,
  t1.p_promonumber,
  t1.gltt_rowid,
  t1.transactionlongname,
  t1.sku_stockcode,
  t1.ac_longname,
  t1.activity,
  t1.ac_code,
  t1.sku_longname,
  t1.sku_profitcentre,
  t1.promotionforecastweek,
  t3.promotionitemstatus;   
  
  commit;
  
	update au_itg.itg_px_weekly_sell 
	set paid_total=0
	where paid_total is null;

	commit;


	
truncate au_sdl.sdl_px_scan_data;

commit;

copy au_sdl.sdl_px_scan_data
(
ac_shortname,  
ac_longname,  
ac_code,  
ac_attribute,  
sc_date,  
sc_scanvolume,  
sc_scanvalue,  
sc_scanprice,  
sku_shortname,  
sku_longname,  
sku_tuncode,  
sku_apncode,  
sku_stockcode,  
ac_salesorg 
) 

from 's3://+context.s3_cdl_bucket_px+/px_scan+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
dateformat 'yyyy-mm-dd'
delimiter ';';


commit;


	
select min(sc_date), max(sc_date) from au_itg.itg_px_scan_data;

	
delete from au_itg.itg_px_scan_data where sc_date >= DATEADD('month', -18, CURRENT_DATE());

DATEADD('month', -18, CURRENT_DATE())

commit;


	

insert into au_itg.itg_px_scan_data
select * from au_sdl.sdl_px_scan_data ;




commit;


	
update au_itg.itg_px_scan_data
   set sku_shortname = '2682308504',
       sku_stockcode = '2682308504'
where sku_stockcode = '2682308501';

commit;


	

truncate au_edw.edw_px_scan_data_fact;

commit;

insert into au_edw.edw_px_scan_data_fact
select * from au_itg.itg_px_scan_data;

commit;




truncate au_sdl.sdl_px_uom;

commit;

copy au_sdl.sdl_px_uom
(
 sku_longname, 
 sku_stockcode, 
 sku_uom, 
 sku_uompersaleable, 
 sku_packspercase
) 

from 's3://+context.s3_cdl_bucket_px+/px_uom+talenddate.getdate(ccyy-mm-dd)+.txt'
credentials 'aws_access_key_id=+context.s3_access_key+;aws_secret_access_key=+context.s3_secret_key+' 
ignoreblanklines 
acceptinvchars 
delimiter ';';


commit;




truncate au_itg.itg_px_uom;
commit;

insert into au_itg.itg_px_uom
select * from au_sdl.sdl_px_uom;

commit;



truncate au_edw.edw_px_uom;
commit;

insert into au_edw.edw_px_uom
select * from au_itg.itg_px_uom;

commit;

sku_longname varchar(40),
sku_stockcode varchar(18),
sku_uom varchar(3),
sku_uompersaleable number(38,0),
sku_packspercase number(38,0)
