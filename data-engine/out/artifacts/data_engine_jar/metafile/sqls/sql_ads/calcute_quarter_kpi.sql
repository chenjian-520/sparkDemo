SELECT
  concat(unix_timestamp(), '-', uuid())                                                                       id,
  output.SiteCodeID                                                                                 site_code,
  output.LevelCodeID                                                                                level_code,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL1_TTL_Manhour), 0), ' ')                                                                                                 online_dl_upph_actual,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL2_Variable_Manhour),  0) ,' '  )                                                                                              offline_var_dl_upph_actual,
  concat(year(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),quarter(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')))
  quarter_id,
  ''                                                                                                          online_dl_upph_target,
  ''                                                                                                          offline_var_dl_upph_target,
  concat(ifnull(sum(Offline_DL_fixed_headcount), 0), '')                                                      offline_fix_dl_headcount_actual,
  ''                                                                                                          offline_fix_dl_headcount_target,
  '${etl_time}'                                                                                               etl_time
FROM dpm_dws_production_output_day output
LEFT JOIN dpm_ods_manual_manhour manhour on
  concat(year(from_unixtime(to_unix_timestamp(manhour.Date, 'yyyyMMdd'), 'yyyy-MM-dd')),quarter(from_unixtime(to_unix_timestamp(manhour.Date, 'yyyyMMdd'), 'yyyy-MM-dd')))
  =
  concat(year(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),quarter(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')))
  AND manhour.Level = output.LevelCodeID
  AND manhour.Site = output.SiteCodeID
GROUP BY output.SiteCodeID,
  output.LevelCodeID,
  concat(year(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),quarter(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')))
