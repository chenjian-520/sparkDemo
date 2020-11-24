select
*
from
  (
    select
      uuid() id ,
      a.site_code site_code,
      if(level_code=='','N/A',a.level_code) level_code,
      a.work_date week_id,
      sum(a.attendance_headcount) attendance_headcount,
      sum(a.onjob_headcount) onjob_headcount,
      (sum(a.attendance_headcount)/sum(a.onjob_headcount))*100 attendance_rate_actual ,
      cast(get_aim_target_by_key(concat_ws('=','D',site_code,level_code,'all', 'all', 'all', 'all'),19) AS FLOAT) attendance_rate_target ,
      '$ETL_TIME$' etl_time
      from
        (
          select
            attendance_headcount,
            attendance_rate_actual,
            attendance_rate_target,
            etl_time,
            id,
            level_code,
            onjob_headcount,
            site_code,
            calculateYearWeek(work_date) work_date
          from
            (select * from attendance_day where work_date != '##' and work_date != '###')
        ) as a
      group by a.site_code , a.level_code,a.work_date
  ) as b
where b.week_id = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24*8)), 'yyyy-MM-dd'))