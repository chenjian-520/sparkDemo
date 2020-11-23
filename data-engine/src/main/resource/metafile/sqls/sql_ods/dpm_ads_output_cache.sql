SELECT
  concat(site_code_id, level_code_id) DimGroupId,
  site_code_id,
  level_code_id,
  site_code,
  level_code
FROM dpm_ads_dim_site
  LEFT JOIN dpm_ads_dim_level ON 1 = 1