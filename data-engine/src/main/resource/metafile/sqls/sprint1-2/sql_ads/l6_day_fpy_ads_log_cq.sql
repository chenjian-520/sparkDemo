SELECT t4.site_code,
       t4.level_code,
       t4.work_dt,
       t4.ict_qty,
       t4.fct_qty,
       t4.pass_qty,
       1 - NVL(t4.ict_qty / t4.pass_qty, 0) as ict,
       1 - NVL(t4.fct_qty / t4.pass_qty, 0) as fct
FROM (SELECT t1.site_code,
             t1.level_code,
             t1.work_dt,
             NVL(ict_qty, 0)  ict_qty,
             NVL(pass_qty, 0) pass_qty,
             NVL(fct_qty, 0)  fct_qty
      FROM (SELECT site_code, level_code, work_dt, NVL(SUM(total_count), 0) pass_qty
            FROM fpyPassStationDay
            WHERE station_code = 'PACKING'
            GROUP BY site_code, level_code, work_dt) t1
             LEFT JOIN (SELECT site_code, level_code, work_dt, SUM(NVL(total_count, 0)) ict_qty
                        FROM fpyRepairStationDay
                        WHERE fail_station IN ('ICT', 'SICT')
                        GROUP BY site_code, level_code, work_dt) t2 ON t1.site_code = t2.site_code
                                                                         AND t1.level_code = t2.level_code
                                                                         AND t1.work_dt = t2.work_dt
             LEFT JOIN (SELECT site_code, level_code, work_dt, SUM(NVL(total_count, 0)) fct_qty
                        FROM fpyRepairStationDay
                        WHERE fail_station IN ('AV', 'DDC', 'FBT', 'FWDL', 'HDCP', 'MAC', 'OFF-LINE', 'OSD TEST', 'USB')
                        GROUP BY site_code, level_code, work_dt) t3 ON t3.site_code = t1.site_code
                                                                         AND t3.level_code = t1.level_code
                                                                         AND t3.work_dt = t1.work_dt) t4
GROUP BY t4.site_code, t4.level_code, t4.work_dt, t4.ict_qty, t4.pass_qty, t4.fct_qty