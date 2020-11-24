SELECT
    concat(unix_timestamp(), '-', uuid()) id,
    month_id                               month_id,
    site_code,
    level_code,
    'N/A'                                 factory_code,
    'Test'                                 process_code,
    customer,
    'N/A'                                 line_code,
    cast(nvl(CASE
         WHEN customer = 'HP'
             THEN
                 nvl(hp_pt_fpy, 0) * nvl(hp_rt_fpy, 0) * 100
         WHEN customer = 'LENOVO'
             THEN
                 nvl(lenovo_fpy, 0) * 100
         END, 0) as FLOAT)                                 fpy_actual,
    cast(get_aim_target_by_key(
             concat_ws("=", 'D', t.site_code, t.level_code, 'all', 'Test', 'all', 'all', customer), 16
         ) AS FLOAT)              fpy_target,
    '$ETL_TIME$' etl_time
FROM
    (
        SELECT
            site_code,
            level_code,
            month_id,
            customer,
            fpy_mark,
            (nvl(input_qty, 0) - nvl(ng_qty, 0)) / nvl(input_qty, 0) fpy_count
        FROM
            (
                SELECT
                    site_code,
                    level_code,
                    month_id,
                    customer,
                    fpy_mark,
                    sum(input_qty) input_qty,
                    sum(ng_qty) ng_qty
                FROM
                    (

                        SELECT
                            t2.site_code,
                            t2.level_code,
                            t2.month_id,
                            t2.customer,
                            CASE
                            WHEN t2.customer_mark = 'T_1'
                                THEN
                                    'hp_pt_fpy'
                            WHEN t2.customer_mark = 'T_2'
                                THEN
                                    'hp_rt_fpy'
                            WHEN t2.customer_mark = 'T_3'
                                THEN
                                    'lenovo_fpy'
                            END                  fpy_mark,
                            nvl(t2.input_qty, 0) input_qty,
                            nvl(t3.ng_qty, 0)    ng_qty
                        FROM
                            (
                                SELECT
                                    site_code,
                                    level_code,
                                    line_code,
                                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
                                    customer,
                                    CASE
                                    WHEN customer = 'HP' AND station_code = 'PRETEST'
                                        THEN 'T_1'
                                    WHEN customer = 'HP' AND station_code = 'POST RUNIN'
                                        THEN 'T_2'
                                    WHEN customer = 'LENOVO' AND station_code = 'Testing'
                                        THEN 'T_3'
                                    ELSE 'T_4'
                                    END                      customer_mark,
                                    sum(nvl(total_count, 0)) input_qty
                                FROM
                                    fpyPassStationDay
                                WHERE level_code = 'L10' AND site_code = 'WH'
                                GROUP BY
                                    site_code,
                                    level_code,
                                    line_code,
                                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                                    customer,
                                    station_code

                            ) t2
                            LEFT JOIN
                            (

                                SELECT
                                    site_code,
                                    level_code,
                                    line_code,
                                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
                                    customer,
                                    CASE
                                    WHEN customer = 'HP' AND fail_station = 'PRETEST'
                                        THEN 'T_1'
                                    WHEN customer = 'HP' AND fail_station = 'POST RUNIN'
                                        THEN 'T_2'
                                    WHEN customer = 'LENOVO' AND fail_station = 'Testing'
                                        THEN 'T_3'
                                    ELSE 'T_4'
                                    END    customer_mark,
                                    nvl(sum(total_count), 0) ng_qty
                                FROM
                                    fpyRepairStationDay
                                WHERE level_code = 'L10' AND site_code = 'WH'
                                GROUP BY
                                    site_code,
                                    level_code,
                                    line_code,
                                    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                                    customer,
                                    fail_station

                            ) t3
                                ON
                                    t2.month_id = t3.month_id AND
                                    t2.site_code = t3.site_code AND
                                    t2.level_code = t3.level_code AND
                                    t2.line_code = t3.line_code AND
                                    t2.customer = t3.customer AND
                                    t2.customer_mark = t3.customer_mark
                    ) tt
                GROUP BY
                    site_code,
                    level_code,
                    month_id,
                    customer,
                    fpy_mark
            ) ttt
    ) t
pivot(
MAX (fpy_count) FOR fpy_mark IN ('hp_pt_fpy', 'hp_rt_fpy', 'lenovo_fpy')
)