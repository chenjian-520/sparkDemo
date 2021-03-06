select
*
from
  (
    select
      uuid() id ,
      a.site_code site_code,
      if(level_code=='','N/A',a.level_code) level_code,
      a.factory_code factory_code,
      a.work_date quarter_id,
      sum(a.attendance_headcount) attendance_headcount,
      sum(a.onjob_headcount) onjob_headcount,
      (sum(a.attendance_headcount)/sum(a.onjob_headcount))*100 attendance_rate_actual ,
      cast(get_aim_target_by_key(concat_ws('=','D',site_code,level_code,factory_code, 'all', 'all', 'all'),19) AS FLOAT) attendance_rate_target ,
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
            factory_code,
            onjob_headcount,
            site_code,
            cast(concat(year(work_date), quarter(work_date)) AS INTEGER) work_date
          from
            attendance_day
        ) as a
      group by a.site_code , a.level_code,a.factory_code,a.work_date
  ) as b
where b.quarter_id = cast(concat(year(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')), quarter(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')) ) AS INTEGER) and site_code = 'WH'