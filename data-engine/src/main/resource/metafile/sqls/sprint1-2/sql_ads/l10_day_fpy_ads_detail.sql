SELECT
  concat(unix_timestamp(), '-', uuid()) id,
  t1.work_date,
  t1.workshift_code,
  t1.site_code,
  t1.level_code,
  t1.line_code,
  'N/A'                                 factory_code,
  'N/A'                                 process_code,
  t1.customer_code,
  cast(nvl(t2.ng_qty,0) AS INTEGER) ng_qty,
  cast(nvl(t1.pass_qty, 0) AS INTEGER) pass_qty,
  '$ETL_TIME$' etl_time,
  t1.station_code
FROM
  (SELECT
     site_code,
     level_code,
     line_code,
     work_dt                  work_date,
     customer                 customer_code,
     station_code,
     work_shift               workshift_code,
     sum(nvl(total_count, 0)) pass_qty
   FROM
     fpyPassStationDay
   WHERE (customer = 'HP' AND station_code = 'PRETEST') OR (customer = 'HP' AND station_code = 'POST RUNIN') OR
         (customer = 'LENOVO' AND station_code = 'Testing')
   GROUP BY
     site_code,
     level_code,
     line_code,
     work_dt,
     customer,
     station_code,
     work_shift
  ) t1
  LEFT JOIN
  (
    SELECT
      site_code,
      level_code,
      line_code,
      work_dt                  work_date,
      customer                 customer_code,
      fail_station             station_code,
      work_shift               workshift_code,
      nvl(SUM(total_count), 0) ng_qty
    FROM
      fpyRepairStationDay
    WHERE (customer = 'HP' AND fail_station = 'PRETEST') OR (customer = 'HP' AND fail_station = 'POST RUNIN') OR
          (customer = 'LENOVO' AND fail_station = 'Testing')
    GROUP BY
      site_code,
      level_code,
      line_code,
      work_dt,
      customer,
      fail_station,
      work_shift
  ) t2
    ON
      t1.site_code = t2.site_code AND
      t1.level_code = t2.level_code AND
      t1.line_code = t2.line_code AND
      t1.customer_code = t2.customer_code AND
      t1.station_code = t2.station_code AND
      t1.workshift_code = t2.workshift_code AND
      t1.work_date = t2.work_date