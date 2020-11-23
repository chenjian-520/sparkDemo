SELECT
  concat(
      '20', ':',
      to_unix_timestamp(L5_6_10.WorkDate, 'yyyy-MM-dd'), ':',
      L5_6_10.Site_Code, ':',
      L5_6_10.Level_Code, ':',
      uuid()
  )                  RowKey,
  L5_6_10.WorkDate   WorkDT,
  L5_6_10.Site_Code  SiteCodeID,
  L5_6_10.Level_Code LevelCodeID,
  cast(L5_6_10.QTY AS VARCHAR(128)) normalized_output_qty,
  cast(unix_timestamp() AS VARCHAR(128))   InsertDT,
  'HS'               InsertBy,
  ''                 UpdateDT,
  ''                 UpdateBy,
  L5_6_10.Level_Code DataFrom
FROM
  (
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
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          sum(dpm_dws_dsn_day_output.L6_QTY) L6_QTY
        FROM (
               SELECT
                 dpm_dws_dsn_day_output.WorkDate                                                 WorkDate,
                 dpm_dws_dsn_day_output.SiteCode                                                 Site_Code,
                 dpm_dws_dsn_day_output.LevelCode                                                Level_Code,
                 nvl(dpm_dws_dsn_day_output.QTY * dpm_ods_manual_normalization.Normalization, 0) L6_QTY
               FROM (
                      SELECT
                        dpm_dws_dsn_day_output.WorkDate,
                        dpm_dws_dsn_day_output.SiteCode,
                        dpm_dws_dsn_day_output.LevelCode,
                        dpm_dws_dsn_day_output.Key,
                        dpm_dws_dsn_day_output.QTY
                      FROM dpm_dws_dsn_day_output
                      WHERE dpm_dws_dsn_day_output.LevelCode = 'L6'
                    ) dpm_dws_dsn_day_output
                 LEFT JOIN dpm_ods_manual_normalization
                   ON dpm_dws_dsn_day_output.Key = dpm_ods_manual_normalization.Key AND
                      dpm_dws_dsn_day_output.LevelCode = dpm_ods_manual_normalization.Level
             ) dpm_dws_dsn_day_output
        GROUP BY
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code
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
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          sum(dpm_dws_dsn_day_output.L5_QTY) L5_QTY
        FROM
          (
            SELECT
              dpm_dws_dsn_day_output.WorkDate                                                 WorkDate,
              dpm_dws_dsn_day_output.SiteCode                                                 Site_Code,
              dpm_dws_dsn_day_output.LevelCode                                                Level_Code,
              nvl(dpm_dws_dsn_day_output.QTY * dpm_ods_manual_normalization.Normalization, 0) L5_QTY
            FROM (
                   SELECT
                     dpm_dws_dsn_day_output.WorkDate,
                     dpm_dws_dsn_day_output.SiteCode,
                     dpm_dws_dsn_day_output.LevelCode,
                     dpm_dws_dsn_day_output.Key,
                     dpm_dws_dsn_day_output.QTY
                   FROM dpm_dws_dsn_day_output
                   WHERE dpm_dws_dsn_day_output.LevelCode = 'L5'
                 ) dpm_dws_dsn_day_output
              LEFT JOIN dpm_ods_manual_normalization
                ON dpm_dws_dsn_day_output.Key = dpm_ods_manual_normalization.Key AND
                   dpm_dws_dsn_day_output.LevelCode = dpm_ods_manual_normalization.Level
          ) dpm_dws_dsn_day_output
        GROUP BY
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code
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
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code,
          sum(dpm_dws_dsn_day_output.L10_QTY) L10_QTY
        FROM
          (
            SELECT
              dpm_dws_dsn_day_output.WorkDate  WorkDate,
              dpm_dws_dsn_day_output.SiteCode  Site_Code,
              dpm_dws_dsn_day_output.LevelCode Level_Code,
              nvl(
                  if(dpm_dws_dsn_day_output.WOType = 'BTO',
                     dpm_dws_dsn_day_output.QTY * dpm_ods_manual_normalization.Normalization_BTO,
                     if(dpm_dws_dsn_day_output.WOType = 'CTO',
                        dpm_dws_dsn_day_output.QTY * dpm_ods_manual_normalization.Normalization_CTO,
                        dpm_dws_dsn_day_output.QTY * dpm_ods_manual_normalization.Normalization
                     )
                  ), 0)                        L10_QTY
            FROM (
                   SELECT
                     dpm_dws_dsn_day_output.WorkDate,
                     dpm_dws_dsn_day_output.SiteCode,
                     dpm_dws_dsn_day_output.LevelCode,
                     dpm_dws_dsn_day_output.Key,
                     dpm_dws_dsn_day_output.QTY,
                     dpm_dws_dsn_day_output.WOType
                   FROM dpm_dws_dsn_day_output
                   WHERE dpm_dws_dsn_day_output.LevelCode = 'L10'
                 ) dpm_dws_dsn_day_output
              LEFT JOIN dpm_ods_manual_normalization
                ON dpm_dws_dsn_day_output.Key = dpm_ods_manual_normalization.Key AND
                   dpm_dws_dsn_day_output.LevelCode = dpm_ods_manual_normalization.Level
          ) dpm_dws_dsn_day_output
        GROUP BY
          dpm_dws_dsn_day_output.WorkDate,
          dpm_dws_dsn_day_output.Site_Code,
          dpm_dws_dsn_day_output.Level_Code
      ) L10_OUTPUT_TB


  ) L5_6_10
