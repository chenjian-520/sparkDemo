select
id ,
site_code,
level_code,
process_code,
line_code,
production_time_quarter,
quarter_id,
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
Oee2DayDateSetQueater.line_code line_code,
a.production_time_quarter production_time_quarter,
Oee2DayDateSetQueater.work_dt_quarter quarter_id,
unix_timestamp() etltime ,
Oee2DayDateSetQueater.oee2_actual actual_output_qty,
nvl((Oee2DayDateSetQueater.oee2_actual/(a.production_time_quarter*3600))*100,0) oee2_actual,
cast(nvl((Oee2DayDateSetQueater.oee2_actual/(a.production_time*3600))*100,0) AS FLOAT) oee1_actual
from Oee2DayDateSetQueater
left join
(
  select * from
  (
    select
    line_code,
    work_dt_quarter,
    sum(planned_downtime_loss_hours) planned_downtime_loss_hours,
    sum(production_time) production_time,
    sum(production_time_day) production_time_quarter
    from
      (
      select
      c.line_code line_code,
      c.work_dt_quarter work_dt_quarter,
      sum(c.planned_downtime_loss_hours) planned_downtime_loss_hours,
      sum(c.production_time) production_time,
      sum(c.production_time_day) production_time_day
      from
        (select
        line_code,
        cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) work_dt_quarter,
        planned_downtime_loss_hours,
        production_time,
        production_time_day
        from
        L6productionEquipment) as c
      group by
      c.line_code,
      c.work_dt_quarter
      )
      group by line_code,work_dt_quarter
    )
  where work_dt_quarter = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER)
)as a
where a.line_code = Oee2DayDateSetQueater.line_code
) as temp







