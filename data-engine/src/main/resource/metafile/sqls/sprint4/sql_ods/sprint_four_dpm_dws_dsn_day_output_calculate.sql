SELECT
  nvl(concat(
          '20', ':',
          concat(to_unix_timestamp(concat(L5_6_10.WorkDate, ' 00:00:00'), 'yyyy-MM-dd HH:mm:ss'), '000'), ':',
          L5_6_10.Site_Code, ':',
          L5_6_10.Level_Code, ':',
          uuid()
      ), 'N/A')                                  Rowkey,
  L5_6_10.Site_Code                              site_code,
  L5_6_10.Level_Code                             level_code,
  L5_6_10.PlantCode                              factory_code,
  L5_6_10.ProcessCode                            process_code,
  L5_6_10.area_code                              area_code,
  L5_6_10.line_code                              line_code,
  L5_6_10.part_no                                part_no,
  L5_6_10.sku                                    sku,
  L5_6_10.plantform                              platform,
  L5_6_10.WOType                                 workorder_type,
  L5_6_10.WorkDate                               work_dt,
  cast(nvl(L5_6_10.UN_NOR_QTY, 0) AS VARCHAR(128))       output_qty,
  cast(nvl(L5_6_10.QTY, 0) AS VARCHAR(128))              normalized_output_qty,
  'process'                                      data_granularity,
  L5_6_10.customer                               customer,
  '$ETL_TIME$' update_dt,
  'HS'                                           update_by,
  'ODS'                                          data_from,
  L5_6_10.work_shift                             work_shift
FROM
  (
    SELECT
      L6_OUTPUT_TB.WorkDate,
      L6_OUTPUT_TB.Site_Code,
      L6_OUTPUT_TB.Level_Code,
      L6_OUTPUT_TB.L6_QTY        QTY,
      L6_OUTPUT_TB.L6_UN_NOR_QTY UN_NOR_QTY,
      2.18                       online_dl_upph_target,
      4.79                       offline_var_dl_upph_target,
      253                        offline_fix_dl_headcount_target,
      L6_OUTPUT_TB.PlantCode,
      L6_OUTPUT_TB.ProcessCode,
      L6_OUTPUT_TB.area_code,
      L6_OUTPUT_TB.line_code,
      L6_OUTPUT_TB.part_no,
      L6_OUTPUT_TB.sku,
      L6_OUTPUT_TB.plantform,
      L6_OUTPUT_TB.WOType,
      L6_OUTPUT_TB.customer,
      L6_OUTPUT_TB.work_shift
    FROM
      (
        SELECT
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          sum(dpm_dws_dsn_day_output.L6_QTY)        L6_QTY,
          sum(dpm_dws_dsn_day_output.L6_UN_NOR_QTY) L6_UN_NOR_QTY,
          dpm_dws_dsn_day_output.PlantCode,
          dpm_dws_dsn_day_output.ProcessCode,
          dpm_dws_dsn_day_output.area_code,
          dpm_dws_dsn_day_output.line_code,
          dpm_dws_dsn_day_output.part_no,
          dpm_dws_dsn_day_output.sku,
          dpm_dws_dsn_day_output.plantform,
          dpm_dws_dsn_day_output.WOType,
          dpm_dws_dsn_day_output.customer,
          dpm_dws_dsn_day_output.work_shift
        FROM (

               SELECT
                 dpm_dws_dsn_day_output.WorkDate                                                         WorkDate,
                 dpm_dws_dsn_day_output.SiteCode                                                         Site_Code,
                 dpm_dws_dsn_day_output.LevelCode                                                        Level_Code,
                 nvl(dpm_dws_dsn_day_output.QTY * nvl( dpm_dim_production_normalized_factor.Normalization, 0), 0) L6_QTY,
                 nvl(dpm_dws_dsn_day_output.QTY, 0)                                                      L6_UN_NOR_QTY,
                 dpm_dws_dsn_day_output.PlantCode,
                 dpm_dws_dsn_day_output.ProcessCode,
                 dpm_dws_dsn_day_output.area_code,
                 dpm_dws_dsn_day_output.line_code,
                 dpm_dws_dsn_day_output.part_no,
                 dpm_dws_dsn_day_output.sku,
                 dpm_dws_dsn_day_output.plantform,
                 dpm_dws_dsn_day_output.WOType,
                 dpm_dws_dsn_day_output.customer,
                 dpm_dws_dsn_day_output.work_shift
               FROM (


                      SELECT
                        dpm_dws_dsn_day_output.work_dt        WorkDate,
                        dpm_dws_dsn_day_output.site_code      SiteCode,
                        dpm_dws_dsn_day_output.level_code     LevelCode,
                        dpm_dws_dsn_day_output.part_no        Key,
                        dpm_dws_dsn_day_output.output_qty     QTY,
                        dpm_dws_dsn_day_output.factory_code   PlantCode,
                        dpm_dws_dsn_day_output.process_code   ProcessCode,
                        dpm_dws_dsn_day_output.area_code,
                        LineTotranfView(dpm_dws_dsn_day_output.line_code) line_code,
                        dpm_dws_dsn_day_output.part_no,
                        dpm_dws_dsn_day_output.sku,
                        dpm_dws_dsn_day_output.platform       plantform,
                        dpm_dws_dsn_day_output.workorder_type WOType,
                        dpm_dws_dsn_day_output.customer,
                        dpm_dws_dsn_day_output.work_shift
                      FROM dpm_dws_dsn_day_output
                      WHERE dpm_dws_dsn_day_output.level_code = 'L6'


                    ) dpm_dws_dsn_day_output
                 LEFT JOIN dpm_dim_production_normalized_factor
                   ON dpm_dws_dsn_day_output.Key = dpm_dim_production_normalized_factor.Key AND
                      dpm_dws_dsn_day_output.LevelCode = dpm_dim_production_normalized_factor.Level AND
                      dpm_dws_dsn_day_output.SiteCode = dpm_dim_production_normalized_factor.site_code

             ) dpm_dws_dsn_day_output
        GROUP BY
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          dpm_dws_dsn_day_output.PlantCode,
          dpm_dws_dsn_day_output.ProcessCode,
          dpm_dws_dsn_day_output.area_code,
          dpm_dws_dsn_day_output.line_code,
          dpm_dws_dsn_day_output.part_no,
          dpm_dws_dsn_day_output.sku,
          dpm_dws_dsn_day_output.plantform,
          dpm_dws_dsn_day_output.WOType,
          dpm_dws_dsn_day_output.customer,
          dpm_dws_dsn_day_output.work_shift
      ) L6_OUTPUT_TB

    UNION ALL

    SELECT
      L5_SFC_OUTPUT_TB.WorkDate,
      L5_SFC_OUTPUT_TB.Site_Code,
      L5_SFC_OUTPUT_TB.Level_Code,
      L5_SFC_OUTPUT_TB.L5_QTY        QTY,
      L5_SFC_OUTPUT_TB.L5_UN_NOR_QTY UN_NOR_QTY,
      1.855                          online_dl_upph_target,
      7.98                           offline_var_dl_upph_target,
      236                            offline_fix_dl_headcount_target,
      L5_SFC_OUTPUT_TB.PlantCode,
      L5_SFC_OUTPUT_TB.ProcessCode,
      L5_SFC_OUTPUT_TB.area_code,
      L5_SFC_OUTPUT_TB.line_code,
      L5_SFC_OUTPUT_TB.part_no,
      L5_SFC_OUTPUT_TB.sku,
      L5_SFC_OUTPUT_TB.plantform,
      L5_SFC_OUTPUT_TB.WOType,
      L5_SFC_OUTPUT_TB.customer,
      L5_SFC_OUTPUT_TB.work_shift
    FROM
      (
        SELECT
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          sum(dpm_dws_dsn_day_output.L5_QTY)        L5_QTY,
          sum(dpm_dws_dsn_day_output.L5_UN_NOR_QTY) L5_UN_NOR_QTY,
          dpm_dws_dsn_day_output.PlantCode,
          dpm_dws_dsn_day_output.ProcessCode,
          dpm_dws_dsn_day_output.area_code,
          dpm_dws_dsn_day_output.line_code,
          dpm_dws_dsn_day_output.part_no,
          dpm_dws_dsn_day_output.sku,
          dpm_dws_dsn_day_output.plantform,
          dpm_dws_dsn_day_output.WOType,
          dpm_dws_dsn_day_output.customer,
          dpm_dws_dsn_day_output.work_shift
        FROM
          (
            SELECT
              dpm_dws_dsn_day_output.WorkDate                                                         WorkDate,
              dpm_dws_dsn_day_output.SiteCode                                                         Site_Code,
              dpm_dws_dsn_day_output.LevelCode                                                        Level_Code,
              nvl(dpm_dws_dsn_day_output.QTY * nvl(dpm_dim_production_normalized_factor.Normalization, 0), 0) L5_QTY,
              nvl(dpm_dws_dsn_day_output.QTY, 0)                                                      L5_UN_NOR_QTY,
              dpm_dws_dsn_day_output.PlantCode,
              dpm_dws_dsn_day_output.ProcessCode,
              dpm_dws_dsn_day_output.area_code,
              dpm_dws_dsn_day_output.line_code,
              dpm_dws_dsn_day_output.part_no,
              dpm_dws_dsn_day_output.sku,
              dpm_dws_dsn_day_output.plantform,
              dpm_dws_dsn_day_output.WOType,
              dpm_dws_dsn_day_output.customer,
              dpm_dws_dsn_day_output.work_shift

            FROM (
                   SELECT
                     dpm_dws_dsn_day_output.work_dt        WorkDate,
                     dpm_dws_dsn_day_output.site_code      SiteCode,
                     dpm_dws_dsn_day_output.level_code     LevelCode,
                     dpm_dws_dsn_day_output.part_no        Key,
                     dpm_dws_dsn_day_output.output_qty     QTY,
                     dpm_dws_dsn_day_output.factory_code   PlantCode,
                     dpm_dws_dsn_day_output.process_code   ProcessCode,
                     dpm_dws_dsn_day_output.area_code,
                     dpm_dws_dsn_day_output.line_code,
                     dpm_dws_dsn_day_output.part_no,
                     dpm_dws_dsn_day_output.sku,
                     dpm_dws_dsn_day_output.platform       plantform,
                     dpm_dws_dsn_day_output.workorder_type WOType,
                     dpm_dws_dsn_day_output.customer,
                     dpm_dws_dsn_day_output.work_shift
                   FROM dpm_dws_dsn_day_output
                   WHERE dpm_dws_dsn_day_output.level_code = 'L5'
                 ) dpm_dws_dsn_day_output
              LEFT JOIN dpm_dim_production_normalized_factor
                ON dpm_dws_dsn_day_output.Key = dpm_dim_production_normalized_factor.Key AND
                   dpm_dws_dsn_day_output.LevelCode = dpm_dim_production_normalized_factor.Level AND
                   dpm_dws_dsn_day_output.SiteCode = dpm_dim_production_normalized_factor.site_code
          ) dpm_dws_dsn_day_output
        GROUP BY
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          dpm_dws_dsn_day_output.PlantCode,
          dpm_dws_dsn_day_output.ProcessCode,
          dpm_dws_dsn_day_output.area_code,
          dpm_dws_dsn_day_output.line_code,
          dpm_dws_dsn_day_output.part_no,
          dpm_dws_dsn_day_output.sku,
          dpm_dws_dsn_day_output.plantform,
          dpm_dws_dsn_day_output.WOType,
          dpm_dws_dsn_day_output.customer,
          dpm_dws_dsn_day_output.work_shift
      ) L5_SFC_OUTPUT_TB

    UNION ALL

    SELECT
      L10_OUTPUT_TB.WorkDate,
      L10_OUTPUT_TB.Site_Code,
      L10_OUTPUT_TB.Level_Code,
      L10_OUTPUT_TB.L10_QTY        QTY,
      L10_OUTPUT_TB.L10_UN_NOR_QTY UN_NOR_QTY,
      2.29                         online_dl_upph_target,
      10.51                        offline_var_dl_upph_target,
      117                          offline_fix_dl_headcount_target,
      L10_OUTPUT_TB.PlantCode,
      L10_OUTPUT_TB.ProcessCode,
      L10_OUTPUT_TB.area_code,
      L10_OUTPUT_TB.line_code,
      L10_OUTPUT_TB.part_no,
      L10_OUTPUT_TB.sku,
      L10_OUTPUT_TB.plantform,
      L10_OUTPUT_TB.WOType,
      L10_OUTPUT_TB.customer,
      L10_OUTPUT_TB.work_shift
    FROM
      (
        SELECT
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          sum(dpm_dws_dsn_day_output.L10_QTY)        L10_QTY,
          sum(dpm_dws_dsn_day_output.L10_UN_NOR_QTY) L10_UN_NOR_QTY,
          dpm_dws_dsn_day_output.PlantCode,
          dpm_dws_dsn_day_output.ProcessCode,
          dpm_dws_dsn_day_output.area_code,
          dpm_dws_dsn_day_output.line_code,
          dpm_dws_dsn_day_output.part_no,
          dpm_dws_dsn_day_output.sku,
          dpm_dws_dsn_day_output.plantform,
          dpm_dws_dsn_day_output.WOType,
          dpm_dws_dsn_day_output.customer,
          dpm_dws_dsn_day_output.work_shift
        FROM
          (
            SELECT
              dpm_dws_dsn_day_output.WorkDate    WorkDate,
              dpm_dws_dsn_day_output.SiteCode    Site_Code,
              dpm_dws_dsn_day_output.LevelCode   Level_Code,
              nvl(
                  if(dpm_dws_dsn_day_output.SiteCode = 'CQ',
                     dpm_dws_dsn_day_output.QTY * dpm_dim_production_normalized_factor.Normalization,
                     if(dpm_dws_dsn_day_output.WOType = 'BTO',
                        dpm_dws_dsn_day_output.QTY * dpm_dim_production_normalized_factor.Normalization_BTO,
                        if(dpm_dws_dsn_day_output.WOType = 'CTO',
                           dpm_dws_dsn_day_output.QTY * dpm_dim_production_normalized_factor.Normalization_CTO,
                           dpm_dws_dsn_day_output.QTY * dpm_dim_production_normalized_factor.Normalization
                        )
                     )
                  ), 0)                          L10_QTY,
              nvl(dpm_dws_dsn_day_output.QTY, 0) L10_UN_NOR_QTY,
              dpm_dws_dsn_day_output.PlantCode,
              dpm_dws_dsn_day_output.ProcessCode,
              dpm_dws_dsn_day_output.area_code,
              dpm_dws_dsn_day_output.line_code,
              dpm_dws_dsn_day_output.part_no,
              dpm_dws_dsn_day_output.sku,
              dpm_dws_dsn_day_output.plantform,
              dpm_dws_dsn_day_output.WOType,
              dpm_dws_dsn_day_output.customer,
              dpm_dws_dsn_day_output.work_shift
            FROM (
                   SELECT
                     dpm_dws_dsn_day_output.work_dt        WorkDate,
                     dpm_dws_dsn_day_output.site_code      SiteCode,
                     dpm_dws_dsn_day_output.level_code     LevelCode,
                     CASE
                     WHEN dpm_dws_dsn_day_output.site_code = 'WH'
                       THEN
                         dpm_dws_dsn_day_output.platform
                     WHEN dpm_dws_dsn_day_output.site_code = 'CQ'
                       THEN
                         dpm_dws_dsn_day_output.part_no
                     ELSE
                       'N/A'
                     END                                   Key,
                     dpm_dws_dsn_day_output.output_qty     QTY,
                     dpm_dws_dsn_day_output.factory_code   PlantCode,
                     dpm_dws_dsn_day_output.process_code   ProcessCode,
                     dpm_dws_dsn_day_output.area_code      area_code,
                     dpm_dws_dsn_day_output.line_code,
                     dpm_dws_dsn_day_output.part_no,
                     dpm_dws_dsn_day_output.sku,
                     dpm_dws_dsn_day_output.platform       plantform,
                     dpm_dws_dsn_day_output.workorder_type WOType,
                     dpm_dws_dsn_day_output.customer,
                     dpm_dws_dsn_day_output.work_shift
                   FROM dpm_dws_dsn_day_output
                   WHERE dpm_dws_dsn_day_output.level_code = 'L10'
                 ) dpm_dws_dsn_day_output
              LEFT JOIN dpm_dim_production_normalized_factor
                ON dpm_dws_dsn_day_output.Key = dpm_dim_production_normalized_factor.Key AND
                   dpm_dws_dsn_day_output.LevelCode = dpm_dim_production_normalized_factor.Level AND
                   dpm_dws_dsn_day_output.SiteCode = dpm_dim_production_normalized_factor.site_code
          ) dpm_dws_dsn_day_output
        GROUP BY
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          dpm_dws_dsn_day_output.PlantCode,
          dpm_dws_dsn_day_output.ProcessCode,
          dpm_dws_dsn_day_output.area_code,
          dpm_dws_dsn_day_output.line_code,
          dpm_dws_dsn_day_output.part_no,
          dpm_dws_dsn_day_output.sku,
          dpm_dws_dsn_day_output.plantform,
          dpm_dws_dsn_day_output.WOType,
          dpm_dws_dsn_day_output.customer,
          dpm_dws_dsn_day_output.work_shift
      ) L10_OUTPUT_TB


  ) L5_6_10
