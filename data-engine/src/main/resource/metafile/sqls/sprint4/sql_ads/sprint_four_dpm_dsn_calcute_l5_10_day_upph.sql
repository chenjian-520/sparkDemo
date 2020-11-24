SELECT
  concat(unix_timestamp(), '-', uuid())               id,
  OUT_QTY.work_dt                                     work_date,
  OUT_QTY.site_code                                   site_code,
  OUT_QTY.level_code                                  level_code,
  OUT_QTY.factory_code                                factory_code,
  OUT_QTY.process_code                                process_code,
  'N/A'                                               line_code,
  'N/A'                                               block_code,
  nvl(PERS_WH.humresource_type, '')                   emp_humman_resource_code,
  cast(round(nvl(CASE
                 WHEN PERS_WH.humresource_type = 'DL1'
                   THEN
                     OUT_QTY.normalized_output_qty
                     / PERS_WH.act_attendance_workhours
                 WHEN PERS_WH.humresource_type = 'DL2V'
                   THEN
                     OUT_QTY.normalized_output_qty
                     / PERS_WH.act_attendance_workhours
                 WHEN PERS_WH.humresource_type = 'DL2F'
                   THEN
                     OUT_QTY.normalized_output_qty
                     / PERS_WH.attendance_qty
                 WHEN PERS_WH.humresource_type = 'IDL'
                   THEN
                     PERS_WH.attendance_qty
                 END, 0), 2) AS FLOAT)
                                                      upph_actual,
  cast(round(nvl(cast(CASE
                      WHEN PERS_WH.humresource_type = 'DL1'
                        THEN
                          get_aim_target_by_key(
                          concat_ws("=", 'D', OUT_QTY.site_code, OUT_QTY.level_code, OUT_QTY.factory_code, OUT_QTY.process_code , 'all', 'all',if(OUT_QTY.site_code='WH' AND OUT_QTY.level_code='L5', 'all', OUT_QTY.customer_code)), 8
                          )
                      WHEN PERS_WH.humresource_type = 'DL2V'
                        THEN
                          get_aim_target_by_key(
                              concat_ws("=", 'D', OUT_QTY.site_code, OUT_QTY.level_code, OUT_QTY.factory_code, OUT_QTY.process_code , 'all', 'all',if(OUT_QTY.site_code='WH' AND OUT_QTY.level_code='L5', 'all', OUT_QTY.customer_code)), 9
                          )
                      WHEN PERS_WH.humresource_type = 'DL2F'
                        THEN
                          get_aim_target_by_key(
                              concat_ws("=", 'D', OUT_QTY.site_code, OUT_QTY.level_code, OUT_QTY.factory_code, OUT_QTY.process_code , 'all', 'all',if(OUT_QTY.site_code='WH' AND OUT_QTY.level_code='L5', 'all', OUT_QTY.customer_code)), 10
                          )
                      WHEN PERS_WH.humresource_type = 'IDL'
                        THEN
                          get_aim_target_by_key(
                              concat_ws("=", 'D', OUT_QTY.site_code, OUT_QTY.level_code, OUT_QTY.factory_code, OUT_QTY.process_code , 'all', 'all',if(OUT_QTY.site_code='WH' AND OUT_QTY.level_code='L5', 'all', OUT_QTY.customer_code)), 11
                          )
                      END AS FLOAT), 0), 2) AS FLOAT) upph_target,
  '$ETL_TIME$'                                        etl_time,
  OUT_QTY.customer_code,
  PERS_WH.humresource_type,
  OUT_QTY.normalized_output_qty                       normalized_output_qty,
  PERS_WH.act_attendance_workhours,
  PERS_WH.attendance_qty,
  OUT_QTY.output_qty
FROM (
       SELECT
         work_dt,
         site_code,
         level_code,
         nvl(factory_code, 'N/A')   factory_code,
         nvl(process_code, 'N/A')   process_code,
         sum(normalized_output_qty) normalized_output_qty,
         sum(output_qty) output_qty,
         customer customer_code
       FROM dwsProductionOutput
       GROUP BY
         work_dt,
         site_code,
         level_code,
         nvl(factory_code, 'N/A'),
         nvl(process_code, 'N/A'),
         customer

     ) OUT_QTY
  LEFT JOIN (
              SELECT
                work_dt,
                site_code,
                level_code,
                nvl(factory_code, 'N/A')      factory_code,
                nvl(process_code, 'N/A')      process_code,
                humresource_type,
                sum(attendance_qty)           attendance_qty,
                sum(act_attendance_workhours) act_attendance_workhours,
                'NULL' customer_code
              FROM dwsPersonnelWorkHours
              GROUP BY work_dt,
                site_code,
                level_code,
                nvl(factory_code, 'N/A'),
                humresource_type,
                nvl(process_code, 'N/A')
            ) PERS_WH
    ON OUT_QTY.work_dt = PERS_WH.work_dt AND
       OUT_QTY.site_code = PERS_WH.site_code AND
       OUT_QTY.level_code = PERS_WH.level_code AND
       OUT_QTY.factory_code = PERS_WH.factory_code AND
       OUT_QTY.process_code = PERS_WH.process_code AND
       OUT_QTY.customer_code = PERS_WH.customer_code
