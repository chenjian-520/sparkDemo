SELECT
  dpm_dws_if_m_dop.WorkDate    WorkDate,
  dpm_dws_if_m_dop.FactoryCode Site_Code,
  dpm_dws_if_m_dop.BUCode      Level_Code,
  dpm_dws_if_m_dop.QTY         L10_QTY,
  dpm_dws_if_m_dop.ModelNo,
  dpm_dws_if_m_dop.PartNo,
  dpm_dws_if_m_dop.WOType      WOType,
  dpm_ods_manual_normalization.Normalization_BTO,
  dpm_ods_manual_normalization.Normalization_CTO,
  dpm_ods_manual_normalization.Normalization

FROM (
       SELECT
         dpm_dws_if_m_dop.WorkDate,
         dpm_dws_if_m_dop.FactoryCode,
         dpm_dws_if_m_dop.BUCode,
         dpm_dws_if_m_dop.ModelNo,
         dpm_dws_if_m_dop.PartNo,
         dpm_dws_if_m_dop.QTY,
         dpm_dws_if_m_dop.WOType
       FROM dpm_dws_if_m_dop
     ) dpm_dws_if_m_dop
  LEFT JOIN dpm_ods_manual_normalization ON (dpm_dws_if_m_dop.ModelNo = dpm_ods_manual_normalization.Key
    OR
                                             dpm_dws_if_m_dop.PartNo = dpm_ods_manual_normalization.Key

                                            ) AND
                                            dpm_dws_if_m_dop.BUCode = dpm_ods_manual_normalization.Level



