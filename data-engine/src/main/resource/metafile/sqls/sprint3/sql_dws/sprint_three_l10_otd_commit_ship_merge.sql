SELECT
  concat(
      '20:',
      concat(cast(to_unix_timestamp(dpm_dwd_production_scm_ship_qty.day_id, 'yyyy-MM-dd') AS VARCHAR(32)), '000'), ':',
      dpm_dwd_production_scm_ship_qty.site_code, ':',
      dpm_dwd_production_scm_ship_qty.level_code, ':',
      dpm_dwd_production_scm_ship_qty.dn_no, ':',
      dpm_dwd_production_scm_ship_qty.dn_item, ':',
      dpm_dwd_production_scm_ship_qty.po_no, ':',
      dpm_dwd_production_scm_ship_qty.po_item, ':',
      dpm_dwd_production_scm_ship_qty.customer)
                                                       Rowkey,
  dpm_dwd_production_scm_ship_qty.day_id               work_dt,
  dpm_dwd_production_scm_ship_qty.site_code,
  dpm_dwd_production_scm_ship_qty.level_code,
  dpm_dwd_production_scm_ship_qty.plant_code,
  dpm_dwd_production_scm_ship_qty.customer,
  dpm_dwd_production_scm_ship_qty.so_no,
  dpm_dwd_production_scm_ship_qty.so_item,
  dpm_dwd_production_scm_ship_qty.po_no,
  dpm_dwd_production_scm_ship_qty.po_item,
  dpm_dwd_production_scm_ship_qty.dn_no,
  dpm_dwd_production_scm_ship_qty.dn_item,
  dpm_dwd_production_scm_ship_qty.customer_pn,
  dpm_dwd_production_scm_ship_qty.shiptocountry,
  cast(nvl(commit_ship_qty, 0) AS VARCHAR(32))         commit_ship_qty,
  cast(nvl(ship_qty, 0) AS VARCHAR(32))                ship_qty,
  concat(cast(unix_timestamp() AS VARCHAR(32)), '000') update_dt,
  'HS'                                                 update_by,
  'DWD'                                                data_from
FROM dpm_dwd_production_scm_ship_qty
  LEFT JOIN dpm_dwd_production_bb_ship_qty
    ON
      dpm_dwd_production_scm_ship_qty.day_id = dpm_dwd_production_bb_ship_qty.day_id AND
      dpm_dwd_production_scm_ship_qty.site_code = dpm_dwd_production_bb_ship_qty.site_code AND
      dpm_dwd_production_scm_ship_qty.level_code = dpm_dwd_production_bb_ship_qty.level_code AND
      dpm_dwd_production_scm_ship_qty.customer = dpm_dwd_production_bb_ship_qty.customer AND
      dpm_dwd_production_scm_ship_qty.po_no = dpm_dwd_production_bb_ship_qty.po_no AND
      dpm_dwd_production_scm_ship_qty.customer_pn = dpm_dwd_production_bb_ship_qty.customer_pn