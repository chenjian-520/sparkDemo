SELECT
  concat(unix_timestamp(), '-', uuid())                 id,
  t1.site_code,
  t1.level_code,
  'N/A'                                                 factory_code,
  process_code                                          process_code,
  customer                                              customer_code,
  'N/A'                                                 workshift_code,
  t1.month_id,
  cast(nvl(t1.uph_actual, 0) AS INTEGER)                                 uph_actual,
  cast(nvl(0, 0) AS INTEGER) uph_target,
  cast(nvl(t1.output_qty_actual, 0) AS INTEGER)                          output_qty_actual,
  cast(nvl(t1.cycle_time_actual, 0) AS INTEGER)                          cycle_time_actual,
  cast(nvl(t1.output_hours, 0) AS INTEGER)                               output_hours,
  cast(t1.uph_adherence_actual  AS FLOAT)                 uph_adherence_actual,
  cast(nvl(get_aim_target_by_key(
               concat_ws('=', 'D', t1.site_code, t1.level_code, 'all', process_code, 'all', 'all', customer), 17
           ), 0) * 100  AS FLOAT)  uph_adherence_target,
  '$ETL_TIME$'                 etl_time,
  c_ct_s ct_output_time,
  c_work_time_s production_total_time
FROM
  (
    SELECT
      site_code,
      level_code,
      process_code,
      customer,
      month_id,
      uph_actual,
      output_qty_actual,
      cycle_time_actual,
      output_hours,
      uph_adherence_actual,
      c_ct_s,
      c_work_time_s
    FROM
      (

        SELECT
          t1.site_code,
          t1.level_code,
          t1.process_code,
          t1.customer,
          t1.month_id                                                                           AS month_id,
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
              'Packing'   process_code,
              customer,
              sum(output_qty) AS c_output_qty,
              sum(ct)         AS c_ct
            FROM uphPartnoDay
            WHERE data_granularity = 'line'
            GROUP BY site_code, level_code, line_code, cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER), customer,area_code
          ) t1
          LEFT JOIN
          (
            SELECT
              site_code,
              level_code,
              line_code,
              area_code,
              customer,
              cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
              sum(work_time) AS c_work_time
            FROM lineInfoDay
            WHERE work_time IS NOT NULL
            GROUP BY site_code, level_code, line_code,customer,cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),area_code
          ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND t2.line_code = t1.line_code
                  AND t2.month_id = t1.month_id AND t1.area_code = t2.area_code  AND t1.customer = t2.customer
        GROUP BY
          t1.site_code, t1.level_code, t1.month_id, t1.customer, t1.process_code
      ) t
    UNION ALL
    SELECT
      site_code,
      level_code,
      process_code,
      customer,
      month_id,
      uph_actual,
      output_qty_actual,
      cycle_time_actual,
      output_hours,
      uph_adherence_actual,
      c_ct_s,
      c_work_time_s
    FROM
      (

        SELECT
          t1.site_code,
          t1.level_code,
          t1.process_code,
          t1.customer,
          t1.month_id                                                                           AS month_id,
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
              'Assy'        process_code,
              customer,
              sum(output_qty) AS c_output_qty,
              sum(ct)         AS c_ct
            FROM uphPartnoDay
            WHERE data_granularity = 'process' AND process_code = 'ASSEMBLY1'
            GROUP BY site_code, level_code, line_code, cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER), customer,area_code
          ) t1
          LEFT JOIN
          (
            SELECT
              site_code,
              level_code,
              line_code,
              area_code,
              customer,
              cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
              sum(work_time) AS c_work_time
            FROM lineInfoDay
            WHERE work_time IS NOT NULL
            GROUP BY site_code, level_code, line_code, customer,cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),area_code
          ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND t2.line_code = t1.line_code
                  AND t2.month_id = t1.month_id AND t1.area_code = t2.area_code AND t1.customer = t2.customer
        GROUP BY
          t1.site_code, t1.level_code, t1.month_id, t1.customer, t1.process_code
      ) t
  ) t1