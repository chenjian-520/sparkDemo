SELECT
  t1.site_code,
  t1.level_code,
  t1.factory_code,
  'ASSEMBLY1' process_code,
  t1.area_code,
  LineTotranfView(t1.line_code) line_code,
  t1.part_no,
  t1.platform,
  t1.work_dt,
  t1.work_shift,
  t1.output_qty,
  nvl(t2.cycle_time, 0) cycle_time,
  t1.customer
FROM
  partnoOutput t1
  LEFT JOIN
  (
    SELECT
      site_code,
      level_code,
      line_code,
      platform,
      cycle_time
    FROM uphCt
  ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND
          if(t1.site_code = 'CQ' AND t1.level_code = 'L10', trim(lower(t2.line_code))='all', trim(lower(t2.line_code)) = trim(lower(t1.line_code)))  AND if(t1.site_code = 'CQ' AND t1.level_code = 'L10', t1.part_no = t2.platform, t2.platform = t1.platform)