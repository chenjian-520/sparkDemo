SELECT
    concat(unix_timestamp(), '-', uuid()) id,
    quarter_id                               quarter_id,
    site_code,
    level_code,
    'N/A'                                 factory_code,
    'Test'                                 process_code,
    customer  customer_code,
    line_code,
   cast(CASE
    WHEN customer = 'HP'
      THEN
        if(nvl(hp_pt_fpy, 0) * nvl(hp_rt_fpy, 0)<0,nvl(hp_pt_fpy, 0) * nvl(hp_rt_fpy, 0)/100,nvl(hp_pt_fpy, 0) * nvl(hp_rt_fpy, 0)) * 100
    WHEN customer = 'LENOVO'
      THEN
        nvl(if(lenovo_fpy<0,lenovo_fpy/100,lenovo_fpy), 0) * 100
    END as FLOAT)                                 fpy_actual,
 cast(
    get_aim_target_by_key(
    concat_ws("=",'D',site_code,level_code,'all', 'all', 'all', 'all'),16
  ) AS FLOAT)                     fpy_target,
    cast(unix_timestamp() AS VARCHAR(32)) etl_time
FROM
    (
        SELECT
            site_code,
            level_code,
            line_code,
            quarter_id,
            customer,
            fpy_mark,
            if(((nvl(input_qty, 0) - nvl(ng_qty, 0)) / nvl(input_qty, 0))<=0,1,((nvl(input_qty, 0) - nvl(ng_qty, 0)) / nvl(input_qty, 0))) fpy_count

        FROM
            (
                SELECT
                    site_code,
                    level_code,
                    line_code,
                    quarter_id,
                    customer,
                    fpy_mark,
                    sum(input_qty) input_qty,
                    sum(ng_qty) ng_qty

                FROM
                    (

                        SELECT
                            t2.site_code,
                            t2.level_code,
                            t2.line_code,
                            t2.quarter_id,
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
                                    cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) quarter_id,
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
                                    cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER),
                                    customer,
                                    station_code

                            ) t2
                            LEFT JOIN
                            (

                                SELECT
                                    site_code,
                                    level_code,
                                    line_code,
                                    cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER) quarter_id,
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
                                    cast(concat(year(work_dt), quarter(work_dt)) AS INTEGER),
                                    customer,
                                    fail_station

                            ) t3
                                ON
                                    t2.quarter_id = t3.quarter_id AND
                                    t2.site_code = t3.site_code AND
                                    t2.level_code = t3.level_code AND
                                    t2.line_code = t3.line_code AND
                                    t2.customer = t3.customer AND
                                    t2.customer_mark = t3.customer_mark
                    ) tt
                GROUP BY
                    site_code,
                    level_code,
                    line_code,
                    quarter_id,
                    customer,
                    fpy_mark
            ) ttt
    ) t
pivot(
MAX (fpy_count) FOR fpy_mark IN ('hp_pt_fpy', 'hp_rt_fpy', 'lenovo_fpy')
)