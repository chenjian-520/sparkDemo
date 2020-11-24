//"work_dt", "site_code", "level_code"
//"act_attendance_qty", "ttl_incumbents_qty" "attendance_rate_actual"
SELECT
  day_dws_personnel_overview.year_id,
  day_dws_personnel_overview.site_code,
  day_dws_personnel_overview.level_code,
  (day_dws_personnel_overview.act_attendance_qty / day_dws_personnel_overview.ttl_incumbents_qty) attendance_rate_actual
FROM (
       SELECT
         year(from_unixtime(to_unix_timestamp(work_dt, 'yyyyMMdd'), 'yyyy-MM-dd')) year_id,
         site_code,
         level_code,
         sum(act_attendance_qty)                                                   act_attendance_qty,
         sum(ttl_incumbents_qty)                                                   ttl_incumbents_qty
       FROM day_dws_personnel_overview
       GROUP BY
         year(from_unixtime(to_unix_timestamp(work_dt, 'yyyyMMdd'), 'yyyy-MM-dd')),
         site_code,
         level_code
     ) day_dws_personnel_overview


