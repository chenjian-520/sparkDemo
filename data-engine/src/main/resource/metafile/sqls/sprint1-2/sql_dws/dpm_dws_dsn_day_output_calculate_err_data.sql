SELECT
  linked_out_put.work_dt,
  linked_out_put.site_code,
  linked_out_put.level_code,
  linked_out_put.sfc_key,
  linked_out_put.linked_key,
  linked_out_put.sfc_qty,
  linked_out_put.factory_code,
  linked_out_put.process_code,
  linked_out_put.area_code,
  linked_out_put.line_code,
  linked_out_put.part_no,
  linked_out_put.sku,
  linked_out_put.plantform,
  linked_out_put.work_order_type,
  linked_out_put.customer,
  linked_out_put.work_shift
FROM
  (
    SELECT
      dpm_dws_dsn_day_output.WorkDate                            work_dt,
      dpm_dws_dsn_day_output.SiteCode                            site_code,
      dpm_dws_dsn_day_output.LevelCode                           level_code,
      dpm_dws_dsn_day_output.Key                                 sfc_key,
      nvl(dpm_dim_production_normalized_factor.Key, 'UN_LINKED') linked_key,
      nvl(dpm_dws_dsn_day_output.QTY, 0)                         sfc_qty,
      dpm_dws_dsn_day_output.PlantCode                           factory_code,
      dpm_dws_dsn_day_output.ProcessCode                         process_code,
      dpm_dws_dsn_day_output.area_code,
      dpm_dws_dsn_day_output.line_code,
      dpm_dws_dsn_day_output.part_no,
      dpm_dws_dsn_day_output.sku,
      dpm_dws_dsn_day_output.plantform,
      dpm_dws_dsn_day_output.WOType                              work_order_type,
      dpm_dws_dsn_day_output.customer,
      dpm_dws_dsn_day_output.work_shift
    FROM (


           SELECT
             dpm_dws_dsn_day_output.WorkDate,
             dpm_dws_dsn_day_output.SiteCode,
             dpm_dws_dsn_day_output.LevelCode,
             dpm_dws_dsn_day_output.Key,
             dpm_dws_dsn_day_output.QTY,
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
           FROM dpm_dws_dsn_day_output
           WHERE dpm_dws_dsn_day_output.LevelCode = 'L6'


         ) dpm_dws_dsn_day_output
      LEFT JOIN dpm_dim_production_normalized_factor
        ON dpm_dws_dsn_day_output.Key = dpm_dim_production_normalized_factor.Key AND
           dpm_dws_dsn_day_output.LevelCode = dpm_dim_production_normalized_factor.Level AND
           dpm_dws_dsn_day_output.SiteCode = dpm_dim_production_normalized_factor.site_code

    UNION ALL

    SELECT
      dpm_dws_dsn_day_output.WorkDate                            work_dt,
      dpm_dws_dsn_day_output.SiteCode                            site_code,
      dpm_dws_dsn_day_output.LevelCode                           level_code,
      dpm_dws_dsn_day_output.Key                                 sfc_key,
      nvl(dpm_dim_production_normalized_factor.Key, 'UN_LINKED') linked_key,
      nvl(dpm_dws_dsn_day_output.QTY, 0)                         sfc_qty,
      dpm_dws_dsn_day_output.PlantCode                           factory_code,
      dpm_dws_dsn_day_output.ProcessCode                         process_code,
      dpm_dws_dsn_day_output.area_code,
      dpm_dws_dsn_day_output.line_code,
      dpm_dws_dsn_day_output.part_no,
      dpm_dws_dsn_day_output.sku,
      dpm_dws_dsn_day_output.plantform,
      dpm_dws_dsn_day_output.WOType                              work_order_type,
      dpm_dws_dsn_day_output.customer,
      dpm_dws_dsn_day_output.work_shift

    FROM (
           SELECT
             dpm_dws_dsn_day_output.WorkDate,
             dpm_dws_dsn_day_output.SiteCode,
             dpm_dws_dsn_day_output.LevelCode,
             dpm_dws_dsn_day_output.Key,
             dpm_dws_dsn_day_output.QTY,
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
           FROM dpm_dws_dsn_day_output
           WHERE dpm_dws_dsn_day_output.LevelCode = 'L5'
         ) dpm_dws_dsn_day_output
      LEFT JOIN dpm_dim_production_normalized_factor
        ON dpm_dws_dsn_day_output.Key = dpm_dim_production_normalized_factor.Key AND
           dpm_dws_dsn_day_output.LevelCode = dpm_dim_production_normalized_factor.Level AND
           dpm_dws_dsn_day_output.SiteCode = dpm_dim_production_normalized_factor.site_code

    UNION ALL

    SELECT
      dpm_dws_dsn_day_output.WorkDate                            work_dt,
      dpm_dws_dsn_day_output.SiteCode                            site_code,
      dpm_dws_dsn_day_output.LevelCode                           level_code,
      nvl(dpm_dws_dsn_day_output.QTY, 0)                         sfc_qty,
      dpm_dws_dsn_day_output.Key                                 sfc_key,
      nvl(dpm_dim_production_normalized_factor.Key, 'UN_LINKED') linked_key,
      dpm_dws_dsn_day_output.PlantCode                           factory_code,
      dpm_dws_dsn_day_output.ProcessCode                         process_code,
      dpm_dws_dsn_day_output.area_code,
      dpm_dws_dsn_day_output.line_code,
      dpm_dws_dsn_day_output.part_no,
      dpm_dws_dsn_day_output.sku,
      dpm_dws_dsn_day_output.plantform,
      dpm_dws_dsn_day_output.WOType                              work_order_type,
      dpm_dws_dsn_day_output.customer,
      dpm_dws_dsn_day_output.work_shift
    FROM (
           SELECT
             dpm_dws_dsn_day_output.WorkDate,
             dpm_dws_dsn_day_output.SiteCode,
             dpm_dws_dsn_day_output.LevelCode,
             dpm_dws_dsn_day_output.Key,
             dpm_dws_dsn_day_output.QTY,
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
           FROM dpm_dws_dsn_day_output
           WHERE dpm_dws_dsn_day_output.LevelCode = 'L10'
         ) dpm_dws_dsn_day_output
      LEFT JOIN dpm_dim_production_normalized_factor
        ON dpm_dws_dsn_day_output.Key = dpm_dim_production_normalized_factor.Key AND
           dpm_dws_dsn_day_output.LevelCode = dpm_dim_production_normalized_factor.Level AND
           dpm_dws_dsn_day_output.SiteCode = dpm_dim_production_normalized_factor.site_code
  ) linked_out_put
WHERE
  linked_out_put.linked_key <> 'UN_LINKED'

