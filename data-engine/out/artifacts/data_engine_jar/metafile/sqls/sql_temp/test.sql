SELECT
  L5_6_10.WorkDate,
  L5_6_10.Site_Code,
  L5_6_10.Level_Code,
  nvl(if(L5_6_10.Level_Code = 'L5', (L5_6_10.QTY + dpm_ods_manual_manhour.Output), L5_6_10.QTY) /
      dpm_ods_manual_manhour.DL1_TTL_Manhour, 0)            online_dl_upph_actual,
  nvl(if(L5_6_10.Level_Code = 'L5', (L5_6_10.QTY + dpm_ods_manual_manhour.Output), L5_6_10.QTY) /
      dpm_ods_manual_manhour.DL2_Variable_Manhour, 0)       offline_var_dl_upph_actual,
  nvl(dpm_ods_manual_manhour.Offline_DL_fixed_headcount, 0) offline_fix_dl_headcount_actual,
  '${etl_by}'                                               etl_by,
  '${etl_time}'                                             etl_time
FROM
  (


    SELECT
      L5_SFC_OUTPUT_TB.WorkDate,
      L5_SFC_OUTPUT_TB.Site_Code,
      L5_SFC_OUTPUT_TB.Level_Code,
      L5_SFC_OUTPUT_TB.L5_QTY QTY
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
              LEFT JOIN dpm_ods_manual_normalization ON dpm_dws_if_m_dop.PartNo = dpm_ods_manual_normalization.Key AND
                                                        dpm_dws_if_m_dop.BUCode = dpm_ods_manual_normalization.Level
          ) dpm_dws_if_m_dop
        GROUP BY
          dpm_dws_if_m_dop.WorkDate,
          dpm_dws_if_m_dop.Site_Code,
          dpm_dws_if_m_dop.Level_Code
      ) L5_SFC_OUTPUT_TB



  ) L5_6_10
  LEFT JOIN (SELECT
               dpm_ods_manual_manhour.Date                            WorkDate,
               dpm_ods_manual_manhour.Site                            Site_Code,
               dpm_ods_manual_manhour.Level                           Level_Code,
               sum(dpm_ods_manual_manhour.Output)                     Output,
               sum(dpm_ods_manual_manhour.DL1_TTL_Manhour)            DL1_TTL_Manhour,
               sum(dpm_ods_manual_manhour.DL2_Variable_Manhour)       DL2_Variable_Manhour,
               sum(dpm_ods_manual_manhour.Offline_DL_fixed_headcount) Offline_DL_fixed_headcount
             FROM dpm_ods_manual_manhour
             GROUP BY dpm_ods_manual_manhour.Date, dpm_ods_manual_manhour.Site,
               dpm_ods_manual_manhour.Level) dpm_ods_manual_manhour ON
                                                                      dpm_ods_manual_manhour.WorkDate
                                                                      =
                                                                      L5_6_10.WorkDate
                                                                      AND
                                                                      dpm_ods_manual_manhour.Site_Code
                                                                      =
                                                                      L5_6_10.Site_Code
                                                                      AND
                                                                      dpm_ods_manual_manhour.Level_Code
                                                                      =
                                                                      L5_6_10.Level_Code