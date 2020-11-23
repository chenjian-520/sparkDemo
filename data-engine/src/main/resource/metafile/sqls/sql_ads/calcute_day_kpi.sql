SELECT
  concat(unix_timestamp(), '-', uuid())                                                   AS id,
  output.WorkDT                                                                           AS work_date,
  output.SiteCodeID                                                                       AS site_code_id,
  output.LevelCodeID                                                                      AS level_code_id,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL1_TTL_Manhour), 0), '') AS online_dl_upph_actual,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL2_Variable_Manhour), 0),
         '')                                                                              AS offline_var_dl_upph_actual,
  (CASE
   WHEN output.LevelCodeID = 'L5'
     THEN '1.855'
   WHEN output.LevelCodeID = 'L6'
     THEN '2.18'
   WHEN output.LevelCodeID = 'L10'
     THEN '2.29'
   ELSE '0'
   END)                                                                                   AS online_dl_upph_target,
  (CASE
   WHEN output.LevelCodeID = 'L5'
     THEN '7.98'
   WHEN output.LevelCodeID = 'L6'
     THEN '4.79'
   WHEN output.LevelCodeID = 'L10'
     THEN '10.51'
   ELSE '0'
   END)                                                                                   AS offline_var_dl_upph_target,
  concat(sum(manhour.Offline_DL_fixed_headcount),
         '')                                                                              AS offline_fix_dl_headcount_actual,
  (CASE
   WHEN output.LevelCodeID = 'L5'
     THEN '236'
   WHEN output.LevelCodeID = 'L6'
     THEN '253'
   WHEN output.LevelCodeID = 'L10'
     THEN '117'
   ELSE '0'
   END)                                                                                   AS offline_fix_dl_headcount_target,
  '${etl_time}'                                                                           AS etl_time
FROM dpm_dws_production_output_day output
  LEFT JOIN dpm_ods_manual_manhour manhour ON manhour.Date = output.WorkDT
                                              AND manhour.Level = output.LevelCodeID
                                              AND manhour.Site = output.SiteCodeID
GROUP BY output.LevelCodeID,
  output.SiteCodeID,
  output.WorkDT

