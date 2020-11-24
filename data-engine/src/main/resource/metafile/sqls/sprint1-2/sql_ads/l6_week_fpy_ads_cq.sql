SELECT concat(unix_timestamp(), '-', uuid())       id,
       t5.week_id                                  week_id,
       t5.site_code,
       t5.level_code,
       'N/A'                                       factory_code,
       'N/A'                                       process_code,
       'N/A'                                       customer_code,
       'N/A'                                       line_code,
       cast(nvl(t5.ict * t5.fct, 0)AS FLOAT) * 100 py_actual,
       nvl(get_aim_target_by_key(
             concat_ws('=', 'D', t5.site_code, t5.level_code, 'all', 'all', 'all', 'all'), 16
               ) * 100, 0)                         fpy_target,
       '$ETL_TIME$'                                etl_time
FROM (SELECT t4.site_code,
             t4.level_code,
             CAST(t4.week_id AS INTEGER)             week_id,
             1 - NVL(t4.ict_qty / t4.pass_qty, 0) as ict,
             1 - NVL(t4.fct_qty / t4.pass_qty, 0) as fct
      FROM (SELECT t1.site_code,
                   t1.level_code,
                   t1.week_id,
                   NVL(ict_qty, 0)  ict_qty,
                   NVL(pass_qty, 0) pass_qty,
                   NVL(fct_qty, 0)  fct_qty
            FROM (SELECT site_code, level_code, calculateYearWeek(work_dt) week_id, NVL(SUM(total_count), 0) pass_qty
                  FROM fpyPassStationDay
                  WHERE station_code = 'PACKING'
                  GROUP BY site_code, level_code, calculateYearWeek(work_dt)) t1
                   LEFT JOIN (SELECT site_code,
                                     level_code,
                                     calculateYearWeek(work_dt) week_id,
                                     SUM(NVL(total_count, 0))   ict_qty
                              FROM fpyRepairStationDay
                              WHERE fail_station IN ('ICT', 'SICT')
                              GROUP BY site_code, level_code, calculateYearWeek(work_dt)) t2
                     ON t1.site_code = t2.site_code
                          AND t1.level_code = t2.level_code
                          AND t1.week_id = t2.week_id
                   LEFT JOIN (SELECT site_code,
                                     level_code,
                                     calculateYearWeek(work_dt) week_id,
                                     SUM(NVL(total_count, 0))   fct_qty
                              FROM fpyRepairStationDay
                              WHERE fail_station IN
                                    ('AV', 'DDC', 'FBT', 'FWDL', 'HDCP', 'MAC', 'OFF-LINE', 'OSD TEST', 'USB')
                              GROUP BY site_code, level_code, calculateYearWeek(work_dt)) t3
                     ON t3.site_code = t1.site_code
                          AND t3.level_code = t1.level_code
                          AND t3.week_id = t1.week_id) t4
      GROUP BY t4.site_code, t4.level_code, t4.week_id, t4.ict_qty, t4.pass_qty, t4.fct_qty) t5
GROUP BY t5.site_code, t5.level_code, t5.week_id, t5.ict, t5.fct
