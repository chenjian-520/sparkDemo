SELECT
  L5_6_10.WorkDate,
  L5_6_10.Site_Code,
  L5_6_10.Level_Code,
  L5_6_10.QTY
FROM
  (
    SELECT
      L6_OUTPUT_TB.WorkDate,
      L6_OUTPUT_TB.Site_Code,
      L6_OUTPUT_TB.Level_Code,
      L6_OUTPUT_TB.L6_QTY QTY,
      2.18 online_dl_upph_target,
      4.79 offline_var_dl_upph_target,
      253 offline_fix_dl_headcount_target
    FROM
      (
        SELECT
          dpm_dws_if_m_dop.WorkDate    WorkDate,
          dpm_dws_if_m_dop.FactoryCode Site_Code,
          dpm_dws_if_m_dop.BUCode      Level_Code,
          ifnull(sum(ifnull(dpm_dws_if_m_dop.QTY, 0)) * sum(ifnull(dpm_ods_manual_normalization.Normalization, 0)),
                 0)                    L6_QTY
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
          LEFT JOIN dpm_ods_manual_normalization ON dpm_dws_if_m_dop.PartNo = dpm_ods_manual_normalization.Key AND
                                                    dpm_dws_if_m_dop.Level = dpm_ods_manual_normalization.Level
        GROUP BY
          dpm_dws_if_m_dop.WorkDate,
          dpm_dws_if_m_dop.FactoryCode,
          dpm_dws_if_m_dop.BUCode
      ) L6_OUTPUT_TB

    UNION ALL

    SELECT
      L5_SFC_OUTPUT_TB.WorkDate,
      L5_SFC_OUTPUT_TB.Site_Code,
      L5_SFC_OUTPUT_TB.Level_Code,
      L5_SFC_OUTPUT_TB.L5_QTY QTY,
      1.855 online_dl_upph_target,
      7.98 offline_var_dl_upph_target,
      236 offline_fix_dl_headcount_target
    FROM
      (
        SELECT
          dpm_dws_if_m_dop.WorkDate    WorkDate,
          dpm_dws_if_m_dop.FactoryCode Site_Code,
          dpm_dws_if_m_dop.BUCode      Level_Code,
          ifnull(sum(ifnull(dpm_dws_if_m_dop.QTY, 0)) * sum(ifnull(dpm_ods_manual_normalization.Normalization, 0)),
                 0)                    L5_QTY
        FROM (
               SELECT
                 WorkDate,
                 FactoryCode,
                 BUCode,
                 PartNo,
                 QTY
               FROM dpm_dws_if_m_dop
               WHERE dpm_dws_if_m_dop.BUCode = 'L5'
             ) dpm_dws_if_m_dop
          LEFT JOIN dpm_ods_manual_normalization ON dpm_dws_if_m_dop.PartNo = dpm_ods_manual_normalization.Key AND
                                                    dpm_dws_if_m_dop.Level = dpm_ods_manual_normalization.Level
        GROUP BY
          dpm_dws_if_m_dop.WorkDate,
          dpm_dws_if_m_dop.FactoryCode,
          dpm_dws_if_m_dop.BUCode
      ) L5_SFC_OUTPUT_TB

    UNION ALL

    SELECT
      L10_OUTPUT_TB.WorkDate,
      L10_OUTPUT_TB.Site_Code,
      L10_OUTPUT_TB.Level_Code,
      L10_OUTPUT_TB.L10_QTY QTY,
      2.29 online_dl_upph_target,
      10.51 offline_var_dl_upph_target,
      117 offline_fix_dl_headcount_target
    FROM
      (
        SELECT
          dpm_dws_if_m_dop.WorkDate    WorkDate,
          dpm_dws_if_m_dop.FactoryCode Site_Code,
          dpm_dws_if_m_dop.BUCode      Level_Code,
          ifnull(sum(ifnull(dpm_dws_if_m_dop.QTY, 0)) * sum(ifnull(dpm_ods_manual_normalization.Normalization, 0)),
                 0)                    L10_QTY
        FROM (
               SELECT
                 WorkDate,
                 FactoryCode,
                 BUCode,
                 ModelNo,
                 QTY
               FROM dpm_dws_if_m_dop
               WHERE dpm_dws_if_m_dop.BUCode = 'L10'
             ) dpm_dws_if_m_dop
          LEFT JOIN dpm_ods_manual_normalization ON dpm_dws_if_m_dop.ModelNo = dpm_ods_manual_normalization.Key AND
                                                    dpm_dws_if_m_dop.Level = dpm_ods_manual_normalization.Level
        GROUP BY
          dpm_dws_if_m_dop.WorkDate,
          dpm_dws_if_m_dop.FactoryCode,
          dpm_dws_if_m_dop.BUCode
      ) L10_OUTPUT_TB
  ) L5_6_10
  LEFT JOIN (SELECT
               dpm_ods_manual_manhour.Date                                       WorkDate,
               dpm_ods_manual_manhour.Site                                       Site_Code,
               dpm_ods_manual_manhour.Level                                      Level_Code,
               sum(ifnull(dpm_ods_manual_manhour.Output, 0))                     Output,
               sum(ifnull(dpm_ods_manual_manhour.DL1_TTL_Manhour, 0))            DL1_TTL_Manhour,
               sum(ifnull(dpm_ods_manual_manhour.DL2_Variable_Manhour, 0))       DL2_Variable_Manhour,
               sum(ifnull(dpm_ods_manual_manhour.Offline_DL_fixed_headcount, 0)) Offline_DL_fixed_headcount
             FROM dpm_ods_manual_manhour
             GROUP BY dpm_ods_manual_manhour.Site, dpm_ods_manual_manhour.Level) dpm_ods_manual_manhour ON
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