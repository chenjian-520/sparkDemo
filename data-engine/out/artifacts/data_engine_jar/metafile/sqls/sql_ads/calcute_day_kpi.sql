SELECT
  concat(unix_timestamp(), '-', uuid()) AS id,
  output.WorkDT AS work_date,
  output.SiteCodeID AS site_code_id,
  output.LevelCodeID AS level_code_id,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL1_TTL_Manhour), 0),  '')         AS online_dl_upph_actual,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL2_Variable_Manhour), 0), '')     AS offline_var_dl_upph_actual,
  '' AS online_dl_upph_target,
  '' AS offline_var_dl_upph_target,
  concat(sum(manhour.Offline_DL_fixed_headcount), '') AS offline_fix_dl_headcount_actual,
  '' AS offline_fix_dl_headcount_target,
  '${etl_time}' AS etl_time
FROM dpm_dws_production_output_day output
LEFT JOIN dpm_ods_manual_manhour manhour on manhour.Date = output.WorkDT
  AND manhour.Level = output.LevelCodeID
  AND manhour.Site = output.SiteCodeID
GROUP BY output.LevelCodeID,
  output.SiteCodeID,
  output.WorkDT

