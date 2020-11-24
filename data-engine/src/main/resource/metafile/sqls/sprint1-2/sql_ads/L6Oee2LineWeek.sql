select
id ,
site_code,
level_code,
process_code,
line_code,
production_time_week,
week_id,
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
Oee2DayDateSetWeek.line_code line_code,
a.production_time_week production_time_week,
Oee2DayDateSetWeek.work_dt_week week_id,
unix_timestamp() etltime ,
Oee2DayDateSetWeek.oee2_actual actual_output_qty,
nvl((Oee2DayDateSetWeek.oee2_actual/(a.production_time_week*3600))*100,0) oee2_actual ,
cast(nvl((Oee2DayDateSetWeek.oee2_actual/(a.production_time*3600))*100,0) AS FLOAT) oee1_actual
from Oee2DayDateSetWeek
left join
(
  select * from
  (
    select
    line_code,
    work_dt_week,
    sum(planned_downtime_loss_hours) planned_downtime_loss_hours,
    sum(production_time) production_time,
    sum(production_time_day) production_time_week
    from
      (
      select
      c.line_code line_code,
      c.work_dt_week work_dt_week,
      sum(c.planned_downtime_loss_hours) planned_downtime_loss_hours,
      sum(c.production_time) production_time,
      sum(c.production_time_day) production_time_day
      from
        (select
        line_code,
        calculateYearWeek(work_dt) work_dt_week,
        planned_downtime_loss_hours,
        production_time,
        production_time_day
        from
        L6productionEquipment) as c
      group by
      c.line_code,
      c.work_dt_week
      )
      group by line_code,work_dt_week
    )
  where work_dt_week = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))
)as a
where a.line_code = Oee2DayDateSetWeek.line_code
)as temp

