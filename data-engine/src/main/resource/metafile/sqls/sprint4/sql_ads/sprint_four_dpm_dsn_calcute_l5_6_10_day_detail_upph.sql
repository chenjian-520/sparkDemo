SELECT
  concat(unix_timestamp(), '-', uuid())                                                      id,
  OUT_QTY.work_dt                                                                            work_date,
  OUT_QTY.site_code                                                                          site_code,
  OUT_QTY.level_code                                                                         level_code,
  OUT_QTY.factory_code                                                                       factory_code,
  OUT_QTY.process_code                                                                      process_code,
  'N/A' line_code,
  'N/A' block_code,
  nvl(PERS_WH.humresource_type,
      'N/A')                                                                                 emp_humman_resource_code,
  OUT_QTY.work_shift                                                                         workshift_code,
  cast(nvl(PERS_WH.attendance_qty, 0) AS INTEGER)                                                             work_headcount,
  cast(nvl(PERS_WH.act_attendance_workhours, 0)        AS INTEGER)                                                         output_hours,
  cast(cast(OUT_QTY.normalized_output_qty AS INTEGER)  AS INTEGER) output_qty_actual,
  '$ETL_TIME$'                                                 etl_time,
  OUT_QTY.customer_code
FROM (
       SELECT
         work_dt,
         site_code,
         level_code,
         if(factory_code = NULL OR factory_code = '', 'N/A', factory_code) factory_code,
         if(process_code = NULL OR process_code = '', 'N/A', process_code) process_code,
         work_shift,
         sum(output_qty)                                                   output_qty,
         sum(normalized_output_qty)                                        normalized_output_qty,
         customer customer_code
       FROM dwsProductionOutput
       GROUP BY
         work_dt,
         site_code,
         level_code,
         if(factory_code = NULL OR factory_code = '', 'N/A', factory_code),
         work_shift,
         if(process_code = NULL OR process_code = '', 'N/A', process_code),
       customer
     ) OUT_QTY
  LEFT JOIN (
              SELECT
                work_dt,
                site_code,
                level_code,
                if(factory_code = NULL OR factory_code = '', 'N/A', factory_code) factory_code,
                if(process_code = NULL OR process_code = '', 'N/A', process_code) process_code,
                work_shift,
                humresource_type,
                sum(attendance_qty)                                               attendance_qty,
                sum(act_attendance_workhours)                                     act_attendance_workhours,
                'NULL' customer_code
              FROM dwsPersonnelWorkHours
              GROUP BY work_dt,
                site_code,
                level_code,
                humresource_type,
                if(factory_code = NULL OR factory_code = '', 'N/A', factory_code),
                work_shift,
                if(process_code = NULL OR process_code = '', 'N/A', process_code)
            ) PERS_WH
    ON OUT_QTY.work_dt = PERS_WH.work_dt AND
       OUT_QTY.site_code = PERS_WH.site_code AND
       OUT_QTY.level_code = PERS_WH.level_code AND
       OUT_QTY.factory_code = PERS_WH.factory_code AND
       OUT_QTY.work_shift = PERS_WH.work_shift AND
        OUT_QTY.process_code = PERS_WH.process_code AND
       OUT_QTY.customer_code = PERS_WH.customer_code