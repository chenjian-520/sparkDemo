SELECT
  concat(unix_timestamp(), '-', uuid())                                           id,
  now_week_total_production_site_data.week_id,
  now_week_total_production_site_data.site_code,
  now_week_total_production_site_data.site_code_desc,
  now_week_total_production_site_data.work_date,
  now_week_total_production_site_data.online_dl_headcount_actual,
  now_week_total_production_site_data.offline_var_dl_headcount_actual,
  now_week_total_production_site_data.offline_fix_dl_headcount_actual,
  now_week_total_production_site_data.idl_headcount_actual,
  now_week_total_production_site_data.turnover_rate_actual,
  now_week_total_production_site_data.turnover_rate_target,
  now_week_total_production_site_data.attendance_rate_actual,
  now_week_total_production_site_data.attendance_rate_target,
  now_week_total_production_site_data.safety_actual,
  now_week_total_production_site_data.safety_target,
  now_week_total_production_site_data.etl_time
FROM
  (
    SELECT
      calculateYearWeek(from_unixtime(to_unix_timestamp(total_production_site_data.work_date, 'yyyyMMdd'), 'yyyy-MM-dd')) week_id,
      total_production_site_data.site_code,
      total_production_site_data.site_code_desc,
      total_production_site_data.work_date,
      total_production_site_data.online_dl_headcount_actual,
      total_production_site_data.offline_var_dl_headcount_actual,
      total_production_site_data.offline_fix_dl_headcount_actual,
      total_production_site_data.idl_headcount_actual,
      total_production_site_data.turnover_rate_actual,
      total_production_site_data.turnover_rate_target,
      total_production_site_data.attendance_rate_actual,
      total_production_site_data.attendance_rate_target,
      total_production_site_data.safety_actual,
      total_production_site_data.safety_target,
      total_production_site_data.etl_time
    FROM total_production_site_data
    WHERE calculateYearWeek(from_unixtime(to_unix_timestamp(total_production_site_data.work_date, 'yyyyMMdd'), 'yyyy-MM-dd')) =
          calculateYearWeek(from_unixtime(to_unix_timestamp(now(), 'yyyyMMdd'), 'yyyy-MM-dd'))
    ORDER BY total_production_site_data.work_date DESC LIMIT 1
  ) now_week_total_production_site_data

UNION

SELECT
  concat(unix_timestamp(), '-', uuid())                                           id,
  pre_week_total_production_site_data.week_id,
  pre_week_total_production_site_data.site_code,
  pre_week_total_production_site_data.site_code_desc,
  pre_week_total_production_site_data.work_date,
  pre_week_total_production_site_data.online_dl_headcount_actual,
  pre_week_total_production_site_data.offline_var_dl_headcount_actual,
  pre_week_total_production_site_data.offline_fix_dl_headcount_actual,
  pre_week_total_production_site_data.idl_headcount_actual,
  pre_week_total_production_site_data.turnover_rate_actual,
  pre_week_total_production_site_data.turnover_rate_target,
  pre_week_total_production_site_data.attendance_rate_actual,
  pre_week_total_production_site_data.attendance_rate_target,
  pre_week_total_production_site_data.safety_actual,
  pre_week_total_production_site_data.safety_target,
  pre_week_total_production_site_data.etl_time
FROM
  (
    SELECT
      calculateYearWeek(from_unixtime(to_unix_timestamp(total_production_site_data.work_date, 'yyyyMMdd'), 'yyyy-MM-dd')) week_id,
      total_production_site_data.site_code,
      total_production_site_data.site_code_desc,
      total_production_site_data.work_date,
      total_production_site_data.online_dl_headcount_actual,
      total_production_site_data.offline_var_dl_headcount_actual,
      total_production_site_data.offline_fix_dl_headcount_actual,
      total_production_site_data.idl_headcount_actual,
      total_production_site_data.turnover_rate_actual,
      total_production_site_data.turnover_rate_target,
      total_production_site_data.attendance_rate_actual,
      total_production_site_data.attendance_rate_target,
      total_production_site_data.safety_actual,
      total_production_site_data.safety_target,
      total_production_site_data.etl_time
    FROM total_production_site_data
    WHERE
          calculateYearWeek(from_unixtime(to_unix_timestamp(total_production_site_data.work_date, 'yyyyMMdd'), 'yyyy-MM-dd')) =
          calculateYearWeek(from_unixtime(to_unix_timestamp(now(), 'yyyyMMdd'), 'yyyy-MM-dd')) - 1
    ORDER BY total_production_site_data.work_date DESC LIMIT 1
  ) pre_week_total_production_site_data