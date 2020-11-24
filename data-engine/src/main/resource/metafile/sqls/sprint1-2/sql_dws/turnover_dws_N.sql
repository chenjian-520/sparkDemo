SELECT
t1.site_code,
t1.level_code,
t1.factory_code,
t1.process_code,
t1.humresource_type,
t1.work_dt,
nvl(ttl_incumbents_qty,0) as ttl_incumbents_qty,
nvl(separation_qty,0) as separation_qty,
nvl(act_attendance_qty,0) as act_attendance_qty,
nvl(plan_attendance_qty,0) as plan_attendance_qty
FROM
  (
    SELECT
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt,
    COUNT(1) AS ttl_incumbents_qty
    FROM empWorkHours
    GROUP BY
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt
  ) t1
LEFT JOIN
  (
    SELECT
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt,
    COUNT(1) AS act_attendance_qty
    FROM empWorkHours
    WHERE onduty_states = 1
    GROUP BY
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt
  ) t2 on t2.site_code = t1.site_code
    AND t2.level_code = t1.level_code
    AND t2.factory_code = t1.factory_code
    AND t2.process_code = t1.process_code
    AND t2.humresource_type = t1.humresource_type
    AND t2.work_dt = t1.work_dt
LEFT JOIN
  (
  SELECT
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    time_of_separation AS work_dt,
    COUNT(1) AS separation_qty
    FROM empTurnover
    GROUP BY
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt
  ) t3 on t3.site_code = t1.site_code
    AND t3.level_code = t1.level_code
    AND t3.factory_code = t1.factory_code
    AND t3.process_code = t1.process_code
    AND t3.humresource_type = t1.humresource_type
    AND t3.work_dt = t1.work_dt
LEFT JOIN
  (
    SELECT
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt,
    COUNT(1) AS plan_attendance_qty
    FROM empWorkHours
    GROUP BY
    site_code,
    level_code,
    factory_code,
    process_code,
    humresource_type,
    work_dt
  ) t4 on t4.site_code = t1.site_code
    AND t4.level_code = t1.level_code
    AND t4.factory_code = t1.factory_code
    AND t4.process_code = t1.process_code
    AND t4.humresource_type = t1.humresource_type
    AND t4.work_dt = t1.work_dt