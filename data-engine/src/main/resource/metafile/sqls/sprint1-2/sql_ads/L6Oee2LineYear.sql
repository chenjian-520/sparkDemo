select
id ,
site_code,
level_code,
process_code,
line_code,
production_time_year,
year_id,
etltime ,
actual_output_qty,
oee2_actual,
cast(get_aim_target_by_key(concat_ws('=','D','WH','L6','all', 'all',line_code, 'all'),15) AS FLOAT)*100 oee2_target,
oee1_actual
from
(
select
uuid() id ,
'WH' site_code,
'L6' level_code,
'SMT/PTH' process_code,
Oee2DayDateSetYear.line_code line_code,
a.production_time_year production_time_year,
Oee2DayDateSetYear.work_dt_year year_id,
unix_timestamp() etltime ,
Oee2DayDateSetYear.oee2_actual actual_output_qty,
nvl((Oee2DayDateSetYear.oee2_actual/(a.production_time_year*3600))*100,0) oee2_actual,
cast(nvl((Oee2DayDateSetYear.oee2_actual/(a.production_time*3600))*100,0) AS FLOAT) oee1_actual
from Oee2DayDateSetYear
left join
(
  select * from
  (
    select
    line_code,
    work_dt_year,
    sum(planned_downtime_loss_hours) planned_downtime_loss_hours,
    sum(production_time) production_time,
    sum(production_time_day) production_time_year
    from
      (
      select
      c.line_code line_code,
      c.work_dt_year work_dt_year,
      sum(c.planned_downtime_loss_hours) planned_downtime_loss_hours,
      sum(c.production_time) production_time,
      sum(c.production_time_day) production_time_day
      from
        (select
        line_code,
        year(work_dt) work_dt_year,
        planned_downtime_loss_hours,
        production_time,
        production_time_day
        from
        L6productionEquipment) as c
      group by
      c.line_code,
      c.work_dt_year
      )
      group by line_code,work_dt_year
    )
  where work_dt_year = year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))
)as a
where a.line_code = Oee2DayDateSetYear.line_code
)as temp







