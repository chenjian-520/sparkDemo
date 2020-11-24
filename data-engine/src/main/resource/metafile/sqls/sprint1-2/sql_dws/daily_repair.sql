SELECT
  t.site_code,
  t.level_code,
  t.area_code,
  t.line_code,
  t.platform,
  t.customer,
  t.work_dt,
  'N/A' work_shift,
  t.part_no,
  t.fail_station,
  sum(t.sn_count) AS total_count
FROM
  (

    SELECT
      nvl(site_code, 'N/A') site_code,
      nvl(level_code, 'N/A') level_code,
      nvl(area_code, 'N/A') area_code,
      nvl(line_code, 'N/A') line_code,
      nvl(work_shift, 'N/A') work_shift,
      nvl(work_dt, 'N/A') work_dt,
      nvl(part_no, 'N/A') part_no,
      nvl(customer, 'N/A') customer,
      nvl(platform, 'N/A') platform,
      nvl(fail_station, 'N/A') fail_station,
      1 sn_count
    FROM dailyRepair

  ) t
GROUP BY
  t.site_code,
  t.level_code,
  t.area_code,
  t.line_code,
  t.work_dt,
  t.part_no,
  t.customer,
  t.platform,
  t.fail_station