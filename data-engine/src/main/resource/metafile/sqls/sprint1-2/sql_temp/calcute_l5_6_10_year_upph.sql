SELECT
  concat(unix_timestamp(), '-', uuid())                                            id,
  L5_6_10.year_id                                                                  year_id,
  L5_6_10.Site_Code                                                                site_code,
  L5_6_10.Level_Code                                                               level_code,
  cast(formatNumber(
           nvl(if(L5_6_10.Level_Code = 'L5', (L5_6_10.QTY + nvl(dpm_ods_manual_manhour.Output, 0)), L5_6_10.QTY) /
               ehr_man_hour.DL1, 0)) AS FLOAT)                                     online_dl_upph_actual,
  cast(formatNumber(
           nvl(if(L5_6_10.Level_Code = 'L5', (L5_6_10.QTY + nvl(dpm_ods_manual_manhour.Output, 0)), L5_6_10.QTY) /
               ehr_man_hour.DL2V, 0)) AS FLOAT)                                    offline_var_dl_upph_actual,
  cast(formatNumber(nvl(ehr_man_hour_last_day_headcount.DL2F, 0)) AS INTEGER)      offline_fix_dl_headcount_actual,
  0                                                                                site_code_id,
  0                                                                                level_code_id,
  ''                                                                               site_code_desc,
  ''                                                                               level_code_desc,
  cast(formatNumber(nvl(L5_6_10.online_dl_upph_target, 0.0)) AS FLOAT)             online_dl_upph_target,
  cast(formatNumber(nvl(L5_6_10.offline_var_dl_upph_target, 0.0)) AS FLOAT)        offline_var_dl_upph_target,
  cast(formatNumber(nvl(L5_6_10.offline_fix_dl_headcount_target, 0)) AS INTEGER)   offline_fix_dl_headcount_target,
  '${etl_time}'                                                                    etl_time,
  cast(formatNumber(nvl(ehr_man_hour_last_day_headcount.IDL, 0)) AS INTEGER)       idl_headcount_actual,
  cast(formatNumber(nvl(ehr_man_hour_last_day_headcount.IDL, 0)) AS INTEGER)       idl_headcount_target,
  CAST(formatNumber(
           nvl(day_oee_fpy.time_efficiency_rate -( day_oee_fpy.performance_efficiency_rate + day_oee_fpy.bad_losses), 0))
       AS FLOAT)                                                                   oee1_actual,
  cast(formatNumber(
           nvl((1 - ((day_oee_fpy.actual_output_qty - day_oee_fpy.good_product_qty) / day_oee_fpy.actual_output_qty)) * 100,
               0)) AS FLOAT)
    oee2_actual,
  cast(formatNumber(
           nvl((1 - ((day_oee_fpy.actual_output_qty - day_oee_fpy.good_product_qty) / day_oee_fpy.actual_output_qty)) * 100,
               0)) AS FLOAT)
    oee2_target
FROM

  (
    SELECT
      year(from_unixtime(to_unix_timestamp(L5_6_10.WorkDate, 'yyyyMMdd'), 'yyyy-MM-dd')) year_id,
      L5_6_10.Site_Code,
      L5_6_10.Level_Code,
      sum(L5_6_10.QTY)                                                                   QTY,
      cast(nvl(max(L5_6_10.online_dl_upph_target), 0) AS VARCHAR(32))                    online_dl_upph_target,
      cast(nvl(max(L5_6_10.offline_var_dl_upph_target), 0) AS VARCHAR(32))               offline_var_dl_upph_target,
      cast(nvl(max(L5_6_10.offline_fix_dl_headcount_target), 0) AS VARCHAR(32))          offline_fix_dl_headcount_target
    FROM (
           SELECT
             L6_OUTPUT_TB.WorkDate,
             L6_OUTPUT_TB.Site_Code,
             L6_OUTPUT_TB.Level_Code,
             L6_OUTPUT_TB.L6_QTY QTY,
             2.18                online_dl_upph_target,
             4.79                offline_var_dl_upph_target,
             253                 offline_fix_dl_headcount_target
           FROM
             (
               SELECT
                 dpm_dws_if_m_dop.WorkDate,
                 dpm_dws_if_m_dop.Site_Code,
                 dpm_dws_if_m_dop.Level_Code,
                 sum(dpm_dws_if_m_dop.L6_QTY) L6_QTY
               FROM (
                      SELECT
                        dpm_dws_if_m_dop.WorkDate                                                 WorkDate,
                        dpm_dws_if_m_dop.FactoryCode                                              Site_Code,
                        dpm_dws_if_m_dop.BUCode                                                   Level_Code,
                        nvl(dpm_dws_if_m_dop.QTY * dpm_ods_manual_normalization.Normalization, 0) L6_QTY
                      FROM (
                             SELECT
                               WorkDate,
                               FactoryCode,
                               BUCode,
                               PartNo,
                               QTY
                             FROM dpm_dws_if_m_dop
                             WHERE dpm_dws_if_m_dop.BUCode = 'L6'
                           ) dpm_dws_if_m_dop
                        LEFT JOIN dpm_ods_manual_normalization
                          ON dpm_dws_if_m_dop.PartNo = dpm_ods_manual_normalization.Key AND
                             dpm_dws_if_m_dop.BUCode = dpm_ods_manual_normalization.Level
                    ) dpm_dws_if_m_dop
               GROUP BY
                 dpm_dws_if_m_dop.WorkDate,
                 dpm_dws_if_m_dop.Site_Code,
                 dpm_dws_if_m_dop.Level_Code
             ) L6_OUTPUT_TB

           UNION ALL

           SELECT
             L5_SFC_OUTPUT_TB.WorkDate,
             L5_SFC_OUTPUT_TB.Site_Code,
             L5_SFC_OUTPUT_TB.Level_Code,
             L5_SFC_OUTPUT_TB.L5_QTY QTY,
             1.855                   online_dl_upph_target,
             7.98                    offline_var_dl_upph_target,
             236                     offline_fix_dl_headcount_target
           FROM
             (
               SELECT
                 dpm_dws_if_m_dop.WorkDate,
                 dpm_dws_if_m_dop.Site_Code,
                 dpm_dws_if_m_dop.Level_Code,
                 sum(dpm_dws_if_m_dop.L5_QTY) L5_QTY
               FROM
                 (
                   SELECT
                     dpm_dws_if_m_dop.WorkDate                                                 WorkDate,
                     dpm_dws_if_m_dop.FactoryCode                                              Site_Code,
                     dpm_dws_if_m_dop.BUCode                                                   Level_Code,
                     nvl(dpm_dws_if_m_dop.QTY * dpm_ods_manual_normalization.Normalization, 0) L5_QTY
                   FROM (
                          SELECT
                            dpm_dws_if_m_dop.WorkDate,
                            dpm_dws_if_m_dop.FactoryCode,
                            dpm_dws_if_m_dop.BUCode,
                            dpm_dws_if_m_dop.PartNo,
                            dpm_dws_if_m_dop.QTY
                          FROM dpm_dws_if_m_dop
                          WHERE dpm_dws_if_m_dop.BUCode = 'L5'
                        ) dpm_dws_if_m_dop
                     LEFT JOIN dpm_ods_manual_normalization
                       ON dpm_dws_if_m_dop.PartNo = dpm_ods_manual_normalization.Key AND
                          dpm_dws_if_m_dop.BUCode = dpm_ods_manual_normalization.Level
                 ) dpm_dws_if_m_dop
               GROUP BY
                 dpm_dws_if_m_dop.WorkDate,
                 dpm_dws_if_m_dop.Site_Code,
                 dpm_dws_if_m_dop.Level_Code
             ) L5_SFC_OUTPUT_TB

           UNION ALL

           SELECT
             L10_OUTPUT_TB.WorkDate,
             L10_OUTPUT_TB.Site_Code,
             L10_OUTPUT_TB.Level_Code,
             L10_OUTPUT_TB.L10_QTY QTY,
             2.29                  online_dl_upph_target,
             10.51                 offline_var_dl_upph_target,
             117                   offline_fix_dl_headcount_target
           FROM
             (
               SELECT
                 dpm_dws_if_m_dop.WorkDate,
                 dpm_dws_if_m_dop.Site_Code,
                 dpm_dws_if_m_dop.Level_Code,
                 sum(dpm_dws_if_m_dop.L10_QTY) L10_QTY
               FROM
                 (
                   SELECT
                     dpm_dws_if_m_dop.WorkDate    WorkDate,
                     dpm_dws_if_m_dop.FactoryCode Site_Code,
                     dpm_dws_if_m_dop.BUCode      Level_Code,
                     nvl(
                         if(dpm_dws_if_m_dop.WOType = 'BTO',
                            dpm_dws_if_m_dop.QTY * dpm_ods_manual_normalization.Normalization_BTO,
                            if(dpm_dws_if_m_dop.WOType = 'CTO',
                               dpm_dws_if_m_dop.QTY * dpm_ods_manual_normalization.Normalization_CTO,
                               dpm_dws_if_m_dop.QTY * dpm_ods_manual_normalization.Normalization
                            )
                         ), 0)                    L10_QTY
                   FROM (
                          SELECT
                            dpm_dws_if_m_dop.WorkDate,
                            dpm_dws_if_m_dop.FactoryCode,
                            dpm_dws_if_m_dop.BUCode,
                            dpm_dws_if_m_dop.ModelNo,
                            dpm_dws_if_m_dop.QTY,
                            dpm_dws_if_m_dop.WOType
                          FROM dpm_dws_if_m_dop
                          WHERE dpm_dws_if_m_dop.BUCode = 'L10'
                        ) dpm_dws_if_m_dop
                     LEFT JOIN dpm_ods_manual_normalization
                       ON dpm_dws_if_m_dop.ModelNo = dpm_ods_manual_normalization.Key AND
                          dpm_dws_if_m_dop.BUCode = dpm_ods_manual_normalization.Level
                 ) dpm_dws_if_m_dop
               GROUP BY
                 dpm_dws_if_m_dop.WorkDate,
                 dpm_dws_if_m_dop.Site_Code,
                 dpm_dws_if_m_dop.Level_Code
             ) L10_OUTPUT_TB


         ) L5_6_10
    GROUP BY
      year(from_unixtime(to_unix_timestamp(L5_6_10.WorkDate, 'yyyyMMdd'), 'yyyy-MM-dd')),
      L5_6_10.Level_Code,
      L5_6_10.Site_Code
  ) L5_6_10


  LEFT JOIN (


              SELECT
                year(from_unixtime(to_unix_timestamp(dpm_ods_manual_manhour.Date, 'yyyyMMdd'), 'yyyy-MM-dd')) year_id,
                dpm_ods_manual_manhour.Site                                                                   Site_Code,
                dpm_ods_manual_manhour.Level                                                                  Level_Code,
                sum(dpm_ods_manual_manhour.Output)                                                            Output,
                sum(
                    dpm_ods_manual_manhour.DL1_TTL_Manhour)                                                   DL1_TTL_Manhour,
                sum(
                    dpm_ods_manual_manhour.DL2_Variable_Manhour)                                              DL2_Variable_Manhour,
                sum(dpm_ods_manual_manhour.Offline_DL_fixed_headcount)                                        Offline_DL_fixed_headcount
              FROM dpm_ods_manual_manhour
              WHERE to_unix_timestamp(dpm_ods_manual_manhour.Date, 'yyyyMMdd') <= to_unix_timestamp(now(), 'yyyyMMdd')
              GROUP BY
                year(from_unixtime(to_unix_timestamp(dpm_ods_manual_manhour.Date, 'yyyyMMdd'), 'yyyy-MM-dd')),
                dpm_ods_manual_manhour.Site,
                dpm_ods_manual_manhour.Level


            ) dpm_ods_manual_manhour ON
                                       dpm_ods_manual_manhour.year_id
                                       =
                                       L5_6_10.year_id
                                       AND
                                       dpm_ods_manual_manhour.Site_Code
                                       =
                                       L5_6_10.Site_Code
                                       AND
                                       dpm_ods_manual_manhour.Level_Code
                                       =
                                       L5_6_10.Level_Code
  LEFT JOIN (
              SELECT
                year(from_unixtime(to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')) year_id,
                ehr_man_hour.SiteCodeID,
                ehr_man_hour.LevelCodeID,
                sum(ehr_man_hour.DL1)                                                                 DL1,
                sum(ehr_man_hour.DL2V)                                                                DL2V,
                sum(ehr_man_hour.DL2F)                                                                DL2F,
                sum(ehr_man_hour.IDL)                                                                 IDL,
                sum(ehr_man_hour.counter)                                                             counter
              FROM (
                     SELECT
                       ehr_man_hour.WorkDT,
                       ehr_man_hour.SiteCodeID,
                       ehr_man_hour.LevelCodeID,
                       ehr_man_hour.DL1,
                       ehr_man_hour.DL2V,
                       ehr_man_hour.DL2F,
                       ehr_man_hour.IDL,
                       1 counter
                     FROM ehr_man_hour
                     WHERE to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd') <= to_unix_timestamp(now(), 'yyyyMMdd')
                   ) ehr_man_hour
              GROUP BY
                year(from_unixtime(to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),
                ehr_man_hour.SiteCodeID,
                ehr_man_hour.LevelCodeID
            ) ehr_man_hour ON
                             ehr_man_hour.year_id
                             =
                             L5_6_10.year_id
                             AND
                             ehr_man_hour.SiteCodeID
                             =
                             L5_6_10.Site_Code
                             AND
                             ehr_man_hour.LevelCodeID
                             =
                             L5_6_10.Level_Code


  LEFT JOIN (
              SELECT
                year(from_unixtime(to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')) year_id,
                ehr_man_hour.SiteCodeID,
                ehr_man_hour.LevelCodeID,
                ehr_man_hour.DL2F,
                ehr_man_hour.IDL,
                ROW_NUMBER()
                OVER (
                  PARTITION BY
                    year(from_unixtime(to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),
                    ehr_man_hour.SiteCodeID,
                    ehr_man_hour.LevelCodeID
                  ORDER BY ehr_man_hour.WorkDT DESC
                  )                                                                                   r
              FROM ehr_man_hour
              WHERE to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd') <= to_unix_timestamp(now(), 'yyyyMMdd')
            ) ehr_man_hour_last_day_headcount
    ON ehr_man_hour_last_day_headcount.year_id = L5_6_10.year_id AND ehr_man_hour_last_day_headcount.r = 1
       AND ehr_man_hour_last_day_headcount.SiteCodeID = L5_6_10.Site_Code
       AND ehr_man_hour_last_day_headcount.LevelCodeID = L5_6_10.Level_Code

  LEFT JOIN (SELECT
               year(from_unixtime(to_unix_timestamp(day_oee_fpy.work_date, 'yyyyMMdd'), 'yyyy-MM-dd')) year_id,
               day_oee_fpy.site_code,
               day_oee_fpy.level_code,
               sum(day_oee_fpy.yield_rate)                                                           yield_rate,
               sum(
                   day_oee_fpy.time_efficiency_rate)                                                 time_efficiency_rate,
               sum(
                   day_oee_fpy.performance_efficiency_rate)                                          performance_efficiency_rate,
               sum(day_oee_fpy.actual_output_qty)                                                    actual_output_qty,
               sum(day_oee_fpy.bad_losses)                                                           bad_losses,
               sum(day_oee_fpy.good_product_qty)                                                     good_product_qty
             FROM day_oee_fpy
             GROUP BY
               year(from_unixtime(to_unix_timestamp(ehr_man_hour.WorkDT, 'yyyyMMdd'), 'yyyy-MM-dd')),
               day_oee_fpy.site_code,
               day_oee_fpy.level_code

            ) day_oee_fpy ON
                            day_oee_fpy.year_id = L5_6_10.year_id
                            AND
                            day_oee_fpy.site_code
                            =
                            L5_6_10.Site_Code
                            AND
                            day_oee_fpy.level_code
                            =
                            L5_6_10.Level_Code