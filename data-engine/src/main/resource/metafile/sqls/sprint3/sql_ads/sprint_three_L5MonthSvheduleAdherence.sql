SELECT *
FROM
  (
    SELECT
      concat(unix_timestamp(), '-', uuid())                                    id,
      dpm_ods_production_planning_day_sc.month_id                              month_id,
      dpm_ods_production_planning_day_sc.site_code,
      dpm_ods_production_planning_day_sc.level_code,
      dpm_ods_production_planning_day_sc.factory_code,
      'N/A'                          process_code,
      'N/A'                                                                    line_code,
      dpm_ods_production_planning_day_sc.customer                              customer_code,
      'N/A'                                                                    workshift_code,
      'N/A'                                                                    work_order,
      cast(nvl(dpm_ods_production_planning_day_sc.schedule_qty, 0) AS INTEGER) work_order_qty,
      cast(nvl(dpm_dws_production_output_dd_mthine.output_qty, 0) AS INTEGER)  output_qty_actual,
      cast(nvl(1 - (
        ((nvl(dpm_ods_production_planning_day_sc.schedule_qty, 0) -
             nvl(dpm_dws_production_output_dd_mthine.output_qty, 0)))
        /
        nvl(dpm_ods_production_planning_day_sc.schedule_qty, 0)
      ), 0) * 100 AS FLOAT)                                                    schedule_adherence,
      '$ETL_TIME$'                                                             etl_time,
      cast(nvl(get_aim_target_by_key(
                   concat_ws("=", 'D', dpm_ods_production_planning_day_sc.site_code,
                             dpm_ods_production_planning_day_sc.level_code,
                             dpm_ods_production_planning_day_sc.factory_code, 'all', 'all', 'all'), 12
               ), 0) * 100 AS FLOAT)                                           schedule_adherence_target
    FROM
      (
        SELECT
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
          site_code,
          level_code,
          if(factory_code = '' OR factory_code = NULL, 'N/A', factory_code)                  factory_code,
          (case process_code
             when '塗裝' then 'Painting'
             when '涂装' then 'Painting'
             when '成型' then 'Molding'
             when '衝壓' then 'Stamping'
             when '沖壓' then 'Stamping'
             when '组装' then 'Assy'
             when '組裝' then 'Assy'
             else 'N/A'
            END
            )  process_code,
          customer,
          sum(nvl(schedule_qty, 0))                                                          schedule_qty
        FROM dpm_ods_production_planning_day
        GROUP BY
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
          site_code,
          level_code,
          if(factory_code = '' OR factory_code = NULL, 'N/A', factory_code),
          process_code,
          customer
      ) dpm_ods_production_planning_day_sc
      LEFT JOIN (
                  SELECT
                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
                    site_code,
                    level_code,
                    if(factory_code = '' OR factory_code = NULL, 'N/A', factory_code)                  factory_code,
                    customer,
                    process_code,
                    sum(nvl(output_qty, 0))                                                            output_qty
                  FROM dpm_dws_production_output_dd
                  GROUP BY
                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                    site_code,
                    level_code,
                    if(factory_code = '' OR factory_code = NULL, 'N/A', factory_code),
                    customer,
                    process_code
                ) dpm_dws_production_output_dd_mthine
        ON
          dpm_ods_production_planning_day_sc.month_id = dpm_dws_production_output_dd_mthine.month_id AND
          dpm_ods_production_planning_day_sc.site_code = dpm_dws_production_output_dd_mthine.site_code AND
          dpm_ods_production_planning_day_sc.level_code = dpm_dws_production_output_dd_mthine.level_code AND
          dpm_ods_production_planning_day_sc.factory_code = dpm_dws_production_output_dd_mthine.factory_code AND
          dpm_ods_production_planning_day_sc.customer = dpm_dws_production_output_dd_mthine.customer AND
          dpm_ods_production_planning_day_sc.process_code = dpm_dws_production_output_dd_mthine.process_code

    UNION ALL

    SELECT
      concat(unix_timestamp(), '-', uuid())                                    id,
      dpm_ods_production_planning_day_sc.month_id                              month_id,
      dpm_ods_production_planning_day_sc.site_code,
      dpm_ods_production_planning_day_sc.level_code,
      'N/A'                                                                    factory_code,
      'N/A'                                                                    process_code,
      'N/A'                                                                    line_code,
      'N/A'                                                                    customer_code,
      'N/A'                                                                    workshift_code,
      'N/A'                                                                    work_order,
      cast(nvl(dpm_ods_production_planning_day_sc.schedule_qty, 0) AS INTEGER) work_order_qty,
      cast(nvl(dpm_dws_production_output_dd_mthine.output_qty, 0) AS INTEGER)  output_qty_actual,
      cast(nvl(1 - (
        abs((nvl(dpm_ods_production_planning_day_sc.schedule_qty, 0) -
             nvl(dpm_dws_production_output_dd_mthine.output_qty, 0)))
        /
        nvl(dpm_ods_production_planning_day_sc.schedule_qty, 0)
      ), 0) * 100 AS FLOAT)                                                    schedule_adherence,
      '$ETL_TIME$'                                                             etl_time,
      cast(nvl(get_aim_target_by_key(
                   concat_ws("=", 'D', dpm_ods_production_planning_day_sc.site_code,
                             dpm_ods_production_planning_day_sc.level_code,
                             'all', 'all', 'all', 'all'), 12
               ), 0) * 100 AS FLOAT)                                           schedule_adherence_target
    FROM
      (
        SELECT
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
          site_code,
          level_code,
          sum(nvl(schedule_qty, 0))                                                          schedule_qty
        FROM dpm_ods_production_planning_day
        GROUP BY
          cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
          site_code,
          level_code
      ) dpm_ods_production_planning_day_sc
      LEFT JOIN (
                  SELECT
                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
                    site_code,
                    level_code,
                    sum(nvl(output_qty, 0))                                                            output_qty
                  FROM dpm_dws_production_output_dd
                  GROUP BY
                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                    site_code,
                    level_code
                ) dpm_dws_production_output_dd_mthine
        ON
          dpm_ods_production_planning_day_sc.month_id = dpm_dws_production_output_dd_mthine.month_id AND
          dpm_ods_production_planning_day_sc.site_code = dpm_dws_production_output_dd_mthine.site_code AND
          dpm_ods_production_planning_day_sc.level_code = dpm_dws_production_output_dd_mthine.level_code

  ) t