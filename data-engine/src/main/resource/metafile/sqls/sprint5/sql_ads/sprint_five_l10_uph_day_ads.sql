SELECT
  concat(unix_timestamp(), '-', uuid())                 id,
  t1.site_code    site_code,
  t1.level_code   level_code,
  'N/A'                                                 factory_code,
  process_code                                          process_code,
  line_code,
  customer                                              customer_code,
  'N/A'                                                 workshift_code,
  t1.work_date,
  nvl(t1.uph_actual, 0)                                 uph_actual,

--   cast(nvl(cast(t4.uph_target AS FLOAT), 0) AS INTEGER) uph_target,
cast(
    get_aim_target_by_key(
    concat_ws("=",'D',site_code,level_code,'all', process_code, line_code, 'all'),17
  ) AS INTEGER) uph_target,

  nvl(t1.output_qty_actual, 0)                          output_qty_actual,
  nvl(t1.cycle_time_actual, 0)                          cycle_time_actual,
  nvl(t1.output_hours, 0)                               output_hours,
cast(nvl(t1.uph_adherence_actual, 0)  AS FLOAT)    uph_adherence_actual,

--   nvl(cast(t4.uph_adherence_target AS FLOAT) * 100, 0)  uph_adherence_target,
cast(
    get_aim_target_by_key(
    concat_ws("=",'D',site_code,level_code,'all', process_code, line_code, 'all'),12
  ) AS FLOAT) uph_adherence_target,
  c_ct_s  ct_output_time,
  c_work_time_s production_total_time,
cast(unix_timestamp() AS VARCHAR(32))   etl_time
FROM
  (
    SELECT
      site_code,
      level_code,
      process_code,
      line_code,
      customer,
      work_date,
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
          t1.line_code,
          t1.customer,
          t1.work_dt                                                                           AS work_date,
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
              work_dt,
              area_code,
              'Packing'   process_code,
              customer,
              sum(output_qty) AS c_output_qty,
              sum(ct)         AS c_ct
            FROM uphPartnoDay
            WHERE data_granularity = 'line'
            GROUP BY site_code, level_code, line_code, work_dt, customer,area_code
          ) t1
          LEFT JOIN
          (
            SELECT
              site_code,
              level_code,
              line_code,
              work_dt,
              area_code,
              customer,
              sum(work_time) AS c_work_time
            FROM lineInfoDay
            WHERE work_time IS NOT NULL
            GROUP BY site_code, level_code, line_code, work_dt,area_code,customer
          ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND t2.line_code = t1.line_code
                  AND t2.work_dt = t1.work_dt AND t1.area_code = t2.area_code AND t1.customer = t2.customer
        GROUP BY
          t1.site_code, t1.level_code, t1.work_dt, t1.customer, t1.process_code ,t1.line_code
      ) t
    UNION ALL
    SELECT
      site_code,
      level_code,
      process_code,
      line_code,
      customer,
      work_date,
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
          t1.line_code,
          t1.customer,
          t1.work_dt                                                                           AS work_date,
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
              work_dt,
              area_code,
              'Assy'        process_code,
              customer,
              sum(output_qty) AS c_output_qty,
              sum(ct)         AS c_ct
            FROM uphPartnoDay
            WHERE data_granularity = 'process' AND process_code = 'ASSEMBLY1'
            GROUP BY site_code, level_code, line_code, work_dt, customer,area_code
          ) t1
          LEFT JOIN
          (
            SELECT
              site_code,
              level_code,
              line_code,
              work_dt,
              area_code,
              customer,
              sum(work_time) AS c_work_time
            FROM lineInfoDay
            WHERE work_time IS NOT NULL
            GROUP BY site_code, level_code, line_code, work_dt,area_code,customer
          ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND t2.line_code = t1.line_code
                  AND t2.work_dt = t1.work_dt AND t1.area_code = t2.area_code AND t1.customer = t2.customer
        GROUP BY
          t1.site_code, t1.level_code, t1.work_dt, t1.customer, t1.process_code ,t1.line_code
      ) t
    UNION ALL
    SELECT
      site_code,
      level_code,
      process_code,
      line_code,
      customer,
      work_date,
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
          t1.line_code,
          t1.customer,
          t1.work_dt                                                                           AS work_date,
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
              work_dt,
              area_code,
              'Testing'        process_code,
              customer,
              sum(output_qty) AS c_output_qty,
              sum(ct)         AS c_ct
            FROM uphPartnoDay
            WHERE data_granularity = 'line' AND process_code = 'PRETEST' or process_code = 'POST RUNIN' or process_code = 'Testing'
            GROUP BY site_code, level_code, line_code, work_dt, customer,area_code
          ) t1
          LEFT JOIN
          (
            SELECT
              site_code,
              level_code,
              line_code,
              work_dt,
              area_code,
              customer,
              sum(work_time) AS c_work_time
            FROM lineInfoDay
            WHERE work_time IS NOT NULL
            GROUP BY site_code, level_code, line_code, work_dt,area_code,customer
          ) t2 ON t2.site_code = t1.site_code AND t2.level_code = t1.level_code AND t2.line_code = t1.line_code
                  AND t2.work_dt = t1.work_dt AND t1.area_code = t2.area_code AND t1.customer = t2.customer
        GROUP BY
          t1.site_code, t1.level_code, t1.work_dt, t1.customer, t1.process_code ,t1.line_code
      ) t
  ) t1
--   LEFT JOIN
--   (
--     SELECT
--       site_code,
--       bu_code                          level_code,
--       cast(max(nvl(uph,
--                    0)) AS VARCHAR(32)) uph_target,
--       cast(max(nvl(schedule_adherence,
--                    0)) AS VARCHAR(32)) uph_adherence_target
--     FROM
--       dpm_ods_production_target_values
--     WHERE
--       site_code = 'WH'
--       AND bu_code = 'L10'
--       AND factory_code = 'all'
--       AND process_code = 'all'
--       AND line_code = 'all'
--       AND machine_id = 'all'
--     GROUP BY
--       site_code,
--       bu_code
--   ) t4 ON t1.site_code = t4.site_code
--           AND t1.level_code = t4.level_code