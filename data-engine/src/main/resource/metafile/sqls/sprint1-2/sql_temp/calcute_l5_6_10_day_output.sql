SELECT
  dpm_dws_if_m_dop.WorkDate,
  dpm_dws_if_m_dop.Site_Code,
  dpm_dws_if_m_dop.Level_Code,
  sum(dpm_dws_if_m_dop.N_L10_QTY) N_L10_QTY,
  sum(dpm_dws_if_m_dop.L10_QTY) L10_QTY,
  dpm_dws_if_m_dop.WOType,
  dpm_dws_if_m_dop.ModelNo,
  dpm_dws_if_m_dop.Key,
  dpm_dws_if_m_dop.Normalization,
  dpm_dws_if_m_dop.Normalization_BTO,
  dpm_dws_if_m_dop.Normalization_CTO
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
          ), 0)                    N_L10_QTY,
      dpm_dws_if_m_dop.QTY         L10_QTY,
      dpm_dws_if_m_dop.WOType,
      dpm_dws_if_m_dop.ModelNo,
      dpm_ods_manual_normalization.Key,
      dpm_ods_manual_normalization.Normalization,
      dpm_ods_manual_normalization.Normalization_BTO,
      dpm_ods_manual_normalization.Normalization_CTO
    FROM (
           SELECT
             dpm_dws_if_m_dop.WorkDate,
             dpm_dws_if_m_dop.FactoryCode,
             dpm_dws_if_m_dop.BUCode,
             dpm_dws_if_m_dop.ModelNo,
             dpm_dws_if_m_dop.QTY,
             dpm_dws_if_m_dop.WOType,
             dpm_dws_if_m_dop.PartNo
           FROM dpm_dws_if_m_dop
           WHERE dpm_dws_if_m_dop.BUCode = 'L10'
         ) dpm_dws_if_m_dop
      LEFT JOIN dpm_ods_manual_normalization ON dpm_dws_if_m_dop.ModelNo = dpm_ods_manual_normalization.Key AND
                                                dpm_dws_if_m_dop.BUCode = dpm_ods_manual_normalization.Level
  ) dpm_dws_if_m_dop
GROUP BY
  dpm_dws_if_m_dop.WorkDate,
  dpm_dws_if_m_dop.Site_Code,
  dpm_dws_if_m_dop.Level_Code,
  dpm_dws_if_m_dop.WOType,
  dpm_dws_if_m_dop.ModelNo,
  dpm_dws_if_m_dop.Key,
  dpm_dws_if_m_dop.Normalization,
  dpm_dws_if_m_dop.Normalization_BTO,
  dpm_dws_if_m_dop.Normalization_CTO



	  
	  
	  
