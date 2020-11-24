SELECT
  concat(unix_timestamp(), '-', uuid())               id,
  cast(OUT_QTY.week_id AS INTEGER)                    week_id,
  OUT_QTY.site_code                                   site_code,
  OUT_QTY.level_code                                  level_code,
  CASE
  WHEN OUT_QTY.level_code = 'L5'
    THEN
      OUT_QTY.factory_code
  ELSE
    'NULL'
  END                                                 factory_code,
  'N/A'                                               process_code,
  'N/A'                                               line_code,
  'N/A'                                               block_code,
  nvl(PERS_WH.humresource_type, '')                   emp_humman_resource_code,
  cast(round(nvl(CASE
                 WHEN PERS_WH.humresource_type = 'DL1'
                   THEN
                     OUT_QTY.normalized_output_qty / PERS_WH.act_attendance_workhours
                 WHEN PERS_WH.humresource_type = 'DL2V'
                   THEN
                     OUT_QTY.normalized_output_qty / PERS_WH.act_attendance_workhours
                 WHEN PERS_WH.humresource_type = 'DL2F'
                   THEN
                     OUT_QTY.normalized_output_qty / PERS_WH.attendance_qty
                 WHEN PERS_WH.humresource_type = 'IDL'
                   THEN
                     PERS_WH.attendance_qty
                 END, 0), 2) AS FLOAT)
                                                      upph_actual,
  cast(
      round(nvl(cast(
                    CASE
                    WHEN PERS_WH.humresource_type = 'DL1' THEN
                      get_aim_target_by_key(
                          concat_ws('=', 'D', OUT_QTY.site_code, OUT_QTY.level_code, if(OUT_QTY.site_code='CQ', 'all', OUT_QTY.factory_code), 'all', 'all', 'all'), 8
                      )
                    WHEN PERS_WH.humresource_type = 'DL2V'
                      THEN
                        get_aim_target_by_key(
                            concat_ws('=', 'D', OUT_QTY.site_code, OUT_QTY.level_code,  if(OUT_QTY.site_code='CQ', 'all', OUT_QTY.factory_code), 'all', 'all', 'all'), 9
                        )
                    WHEN PERS_WH.humresource_type = 'DL2F'
                      THEN
                        get_aim_target_by_key(
                            concat_ws('=', 'D', OUT_QTY.site_code, OUT_QTY.level_code,  if(OUT_QTY.site_code='CQ', 'all', OUT_QTY.factory_code), 'all', 'all', 'all'), 10
                        )
                    WHEN PERS_WH.humresource_type = 'IDL'
                      THEN
                        get_aim_target_by_key(
                            concat_ws('=', 'D', OUT_QTY.site_code, OUT_QTY.level_code,  if(OUT_QTY.site_code='CQ', 'all', OUT_QTY.factory_code), 'all', 'all', 'all'), 11
                        )
                    END
                    AS FLOAT)
      , 0), 2)
      AS FLOAT) upph_target,
  '$ETL_TIME$'                                        etl_time,
  PERS_WH.humresource_type,
  OUT_QTY.normalized_output_qty                       normalized_output_qty,
  PERS_WH.act_attendance_workhours,
  PERS_WH.attendance_qty,
  OUT_QTY.output_qty
FROM (
       SELECT
         calculateYearWeek(work_dt) week_id,
         site_code,
         level_code,
         nvl(factory_code, 'N/A')   factory_code,
         sum(normalized_output_qty) normalized_output_qty,
         sum(output_qty) output_qty
       FROM dwsProductionOutput
       GROUP BY
         calculateYearWeek(work_dt),
         site_code,
         level_code,
         nvl(factory_code, 'N/A')

     ) OUT_QTY
  LEFT JOIN (
              SELECT
                calculateYearWeek(work_dt)    week_id,
                site_code,
                level_code,
                nvl(factory_code, 'N/A')      factory_code,
                humresource_type,
                sum(attendance_qty)           attendance_qty,
                sum(act_attendance_workhours) act_attendance_workhours
              FROM dwsPersonnelWorkHours
              GROUP BY
                calculateYearWeek(work_dt),
                site_code,
                level_code,
                nvl(factory_code, 'N/A'),
                humresource_type
            ) PERS_WH
    ON OUT_QTY.week_id = PERS_WH.week_id AND
       OUT_QTY.site_code = PERS_WH.site_code AND
       OUT_QTY.level_code = PERS_WH.level_code AND
       OUT_QTY.factory_code = PERS_WH.factory_code
