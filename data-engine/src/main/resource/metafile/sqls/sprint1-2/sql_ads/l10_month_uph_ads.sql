SELECT
  concat(unix_timestamp(), '-', uuid())                id,
  t1.site_code,
  t1.level_code,
  'N/A'                                                 factory_code,
  'N/A'                                                 process_code,
  'N/A'                                                 customer_code,
  'N/A'                                                 workshift_code,
  t1.month_id,
  nvl(t1.uph_actual, 0)                                uph_actual,
  cast(0 AS INTEGER)                 uph_target,
  nvl(t1.output_qty_actual, 0)                         output_qty_actual,
  cast(nvl(nvl(t1.cycle_time_actual, 0)  , 0) AS INTEGER)                        cycle_time_actual,
  cast(nvl(nvl(t1.output_hours, 0) , 0) AS INTEGER)                               output_hours,
  cast(t1.uph_adherence_actual  AS FLOAT)                 uph_adherence_actual,
  cast(nvl(get_aim_target_by_key(
               concat_ws('=', 'D', t1.site_code, t1.level_code, 'all', 'all', 'all', 'all'), 17
           ), 0) * 100 AS FLOAT)  uph_adherence_target,
  '$ETL_TIME$'                etl_time,
  c_ct_s ct_output_time,
  c_work_time_s production_total_time
FROM
  (
    SELECT
      t1.site_code,
      t1.level_code,
      t1.month_id                                                                          AS month_id,
      cast(nvl(bround(sum(nvl(c_output_qty, 0)) / 24, 4), 0) AS INTEGER)                   AS uph_actual,
      cast(nvl(sum(nvl(c_output_qty, 0)), 0) AS INTEGER)                                   AS output_qty_actual,
      cast(nvl(bround(3600 / bround(sum(nvl(c_output_qty, 0)) / 24, 4), 4), 0) AS INTEGER) AS cycle_time_actual,
      24                                                                                   AS output_hours,
      cast(
          nvl(
              bround(
                  ((sum(nvl(c_ct, 0))) / (sum(nvl(c_work_time, 0)))) * 100
                  , 4)
              , 0)
          AS FLOAT)                                                                        AS uph_adherence_actual,
      sum(nvl(c_ct, 0)) c_ct_s,
      sum(nvl(c_work_time, 0)) c_work_time_s
    FROM
      (
        SELECT
          site_code,
          level_code,
          line_code,
          area_code,
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
          sum(output_qty) AS                                                                 c_output_qty,
          sum(ct)         AS                                                                 c_ct
        FROM uphPartnoDay
        GROUP BY site_code, level_code, line_code,
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER), area_code
      ) t1
      LEFT JOIN
      (
        SELECT
          site_code,
          level_code,
          line_code,
          area_code,
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
          sum(work_time) AS                                                                  c_work_time
        FROM lineInfoDay
        WHERE work_time IS NOT NULL
        GROUP BY site_code, level_code, line_code,
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),area_code
      ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND t2.line_code = t1.line_code
              AND t2.month_id = t1.month_id AND t1.area_code = t2.area_code
    GROUP BY
      t1.site_code, t1.level_code, t1.month_id

  ) t1