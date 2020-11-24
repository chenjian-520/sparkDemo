SELECT
  concat(unix_timestamp(), '-', uuid())                                                         id,
  concat(year(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),
         weekofyear(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd'))) week_id,
  output.SiteCodeID                                                                             site_code_id,
  output.LevelCodeID                                                                            level_code_id,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL1_TTL_Manhour), 0),
         '')                                                                                    online_dl_upph_actual,
  concat(ifnull(sum(output.normalized_output_qty) / sum(manhour.DL2_Variable_Manhour), 0),
         '')                                                                                    offline_var_dl_upph_actual,
  (CASE
   WHEN output.LevelCodeID = 'L5'
     THEN '1.855'
   WHEN output.LevelCodeID = 'L6'
     THEN '2.18'
   WHEN output.LevelCodeID = 'L10'
     THEN '2.29'
   ELSE '0'
   END) AS                                                                                      online_dl_upph_target,
  (CASE
   WHEN output.LevelCodeID = 'L5'
     THEN '7.98'
   WHEN output.LevelCodeID = 'L6'
     THEN '4.79'
   WHEN output.LevelCodeID = 'L10'
     THEN '10.51'
   ELSE '0'
   END) AS                                                                                      offline_var_dl_upph_target,
  concat(ifnull(sum(Offline_DL_fixed_headcount), 0),
         '')                                                                                    offline_fix_dl_headcount_actual,
  (CASE
   WHEN output.LevelCodeID = 'L5'
     THEN '236'
   WHEN output.LevelCodeID = 'L6'
     THEN '253'
   WHEN output.LevelCodeID = 'L10'
     THEN '117'
   ELSE '0'
   END) AS                                                                                      offline_fix_dl_headcount_target,
  '${etl_time}'                                                                                 etl_time
FROM dpm_dws_production_output_day output
  LEFT JOIN dpm_ods_manual_manhour manhour ON
                                             concat(year(from_unixtime(to_unix_timestamp(manhour.Date, 'yyyyMMdd'),
                                                                       'yyyy-MM-dd')), weekofyear(
                                                        from_unixtime(to_unix_timestamp(manhour.Date, 'yyyyMMdd'),
                                                                      'yyyy-MM-dd')))
                                             =
                                             concat(year(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'),
                                                                       'yyyy-MM-dd')), weekofyear(
                                                        from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'),
                                                                      'yyyy-MM-dd')))
                                             AND manhour.Level = output.LevelCodeID
                                             AND manhour.Site = output.SiteCodeID
GROUP BY output.SiteCodeID,
  output.LevelCodeID,
  concat(year(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),
         weekofyear(from_unixtime(to_unix_timestamp(output.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')))

