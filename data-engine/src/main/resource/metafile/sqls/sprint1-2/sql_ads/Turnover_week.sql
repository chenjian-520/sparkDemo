select
  id,
  site_code,
  level_code,
  week_id,
  turnover_headcount,
  onjob_headcount,
  turnover_rate_actual,
  cast(get_aim_target_by_key(concat_ws("=",'D',site_code,level_code,'all', 'all', 'all', 'all'),18) AS FLOAT)*100*6 turnover_rate_target,
  unix_timestamp() etl_time
from
  (
    select
      uuid() id,
      a.site_code site_code,
      a.level_code level_code,
      a.week_id week_id,
      a.turnover_headcount,
      b.onjob_headcount,
      a.turnover_headcount/(b.onjob_headcount+a.turnover_headcount)*100 turnover_rate_actual
    from
      (
        select
        site_code,
        if(level_code=='','all',level_code) level_code,
        work_date week_id,
        sum(turnover_headcount) turnover_headcount
      from
        (
          select
            etl_time,
            id,
            level_code,
            onjob_headcount,
            site_code,
            turnover_headcount,
            turnover_rate_actual,
            calculateYearWeek(work_date) work_date
          from
          turnover_day
        )
      group by
        site_code,
        level_code,
        work_date
      ) a
    left join
      (
        select
          site_code,
          if(level_code=='','N/A',level_code) level_code,
          sum(onjob_headcount) onjob_headcount,
          work_date
        from
          turnover_day
        where work_date = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')
        group by
          site_code,
          level_code,
          work_date
      ) b
      on a.site_code = b.site_code and a.level_code = b.level_code
  )
where  week_id = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))


