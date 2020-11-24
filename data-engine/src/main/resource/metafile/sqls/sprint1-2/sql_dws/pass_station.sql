SELECT
  t.site_code,
  t.level_code,
  t.factory_code,
  t.process_code,
  t.area_code,
  t.line_code,
  t.sku,
  t.plantform,
  t.customer,
  t.work_dt,
  t.work_shift,
  t.part_no,
  t.station_code,
  t.station_name,
  sum(nvl(t.oneCount, 0)) AS total_count,
  sum(nvl(t.is_fail, 0))  AS fail_count
FROM
  (
    SELECT
      site_code,
      level_code,
      factory_code,
      process_code,
      area_code,
      line_code,
      sku,
      plantform,
      customer,
      work_dt,
      work_shift,
      part_no,
      station_code,
      station_name,
      1                       oneCount,
      cast(is_fail AS BIGINT) is_fail
    FROM passStation
  ) t
GROUP BY
  t.site_code,
  t.level_code,
  t.factory_code,
  t.process_code,
  t.area_code,
  t.line_code,
  t.sku,
  t.plantform,
  t.customer,
  t.work_dt,
  t.work_shift,
  t.part_no,
  t.station_code,
  t.station_name



