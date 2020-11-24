select * from
(
  select
  a.work_dt work_dt,
  a.station_code station_code,
  a.line_code line_code,
  sum(a.fail_count) fail_count,
  sum(a.total_count) total_count
  from
      (
        select
        line_code,
        station_code,
        calculateYearWeek(work_dt) work_dt,
        fail_count,
        total_count
        from L6FpyLineDayView
        ) as a
  group by a.work_dt,a.station_code,a.line_code
)
where work_dt = calculateYearWeek(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd'))

