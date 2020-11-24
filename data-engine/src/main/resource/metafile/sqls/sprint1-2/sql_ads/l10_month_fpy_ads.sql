SELECT
    concat(unix_timestamp(), '-', uuid()) id,
    t1.month_id                            month_id,
    t1.site_code,
    t1.level_code,
    'N/A'                                 factory_code,
    'N/A'                                 process_code,
    'N/A'                                 customer_code,
    'N/A'                                 line_code,
    cast(
        nvl(
            (
                nvl(
                    hp_output_qty *
                    (
                        ((hp_pt_input_qty - hp_pt_ng_qty) / hp_pt_input_qty) *
                        ((hp_rt_input_qty - hp_rt_ng_qty) / hp_rt_input_qty)
                    )
                    , 0)
                +
                nvl(lenovo_output_qty *
                    (
                        (lenovo_input_qty - lenovo_ng_qty) / lenovo_input_qty
                    )
                , 0)
            )
            / (hp_output_qty + lenovo_output_qty)
            , 0)
        AS FLOAT) * 100                   fpy_actual,
    nvl(get_aim_target_by_key(
            concat_ws('=', 'D', t1.site_code, t1.level_code, 'all', 'all', 'all', 'all'), 16
        ) * 100, 0)              fpy_target,
    '$ETL_TIME$' etl_time,
    hp_output_qty,
    hp_pt_input_qty,
    hp_rt_input_qty,
    hp_rt_ng_qty,
    hp_rt_input_qty,
    lenovo_output_qty,
    lenovo_input_qty,
    lenovo_ng_qty
FROM (
         SELECT
             t5.site_code,
             t5.level_code,
             CAST(t5.month_id AS INTEGER) month_id,
             sum(t5.hp_output_qty)     hp_output_qty,
             sum(t5.hp_pt_input_qty)   hp_pt_input_qty,
             sum(t5.hp_pt_ng_qty)      hp_pt_ng_qty,
             sum(t5.hp_rt_input_qty)   hp_rt_input_qty,
             sum(t5.hp_rt_ng_qty)      hp_rt_ng_qty,
             sum(t5.lenovo_output_qty) lenovo_output_qty,
             sum(t5.lenovo_input_qty)  lenovo_input_qty,
             sum(t5.lenovo_ng_qty)     lenovo_ng_qty
         FROM
             (
                 SELECT
                     t1.site_code,
                     t1.level_code,
                     t1.month_id,
                     nvl(hp_output_qty, 0)     hp_output_qty,
                     nvl(hp_pt_input_qty, 0)   hp_pt_input_qty,
                     nvl(hp_pt_ng_qty, 0)      hp_pt_ng_qty,
                     nvl(hp_rt_input_qty, 0)   hp_rt_input_qty,
                     nvl(hp_rt_ng_qty, 0)      hp_rt_ng_qty,
                     nvl(lenovo_output_qty, 0) lenovo_output_qty,
                     nvl(lenovo_input_qty, 0)  lenovo_input_qty,
                     nvl(lenovo_ng_qty, 0)     lenovo_ng_qty
                 FROM
                     (
                         SELECT
                             site_code,
                             level_code,
                             month_id,
                             sum(nvl(HP, 0))     hp_output_qty,
                             sum(nvl(LENOVO, 0)) lenovo_output_qty
                         FROM
                             (
                                 SELECT
                                     site_code,
                                     level_code,
                                     cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
                                     customer,
                                     sum(nvl(output_qty, 0)) output_qty
                                 FROM
                                     fpyOutPutDay
                                 WHERE
                                     level_code = 'L10' AND site_code = 'WH'
                                     AND (  customer = 'HP' OR customer = 'LENOVO' )
                                 GROUP BY
                                     site_code,
                                     level_code,
                                     cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                                     customer

                             ) temp
                         pivot  ( SUM (output_qty) FOR customer IN ('HP', 'LENOVO') )
                         GROUP BY
                         site_code,
                         level_code,
                         month_id
                     ) t1
                     LEFT JOIN
                     (
                         SELECT
                             site_code,
                             level_code,
                             month_id,
                             sum(nvl(T_1, 0)) hp_pt_input_qty,
                             sum(nvl(T_2, 0)) hp_rt_input_qty,
                             sum(nvl(T_3, 0)) lenovo_input_qty
                         FROM
                             (
                                 SELECT
                                     *
                                 FROM
                                     (
                                         SELECT
                                             site_code,
                                             level_code,
                                             cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
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
                                             cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                                             customer,
                                             station_code
                                     )t
                                 WHERE t.customer_mark <> 'T_4'

                             ) temp
                         pivot( SUM (input_qty) FOR customer_mark IN ('T_1', 'T_2', 'T_3') )
                         GROUP BY
                         site_code,
                         level_code,
                         month_id
                     ) t2
                         ON t2.site_code = t1.site_code
                            AND t2.level_code = t1.level_code
                            AND t2.month_id = t1.month_id
                     LEFT JOIN
                     (
                         SELECT
                             site_code,
                             level_code,
                             month_id,
                             sum(nvl(T_1, 0)) hp_pt_ng_qty,
                             sum(nvl(T_2, 0)) hp_rt_ng_qty,
                             sum(nvl(T_3, 0)) lenovo_ng_qty
                         FROM (
                                  SELECT
                                      *
                                  FROM
                                      (
                                          SELECT
                                              site_code,
                                              level_code,
                                              cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) month_id,
                                              CASE
                                              WHEN customer = 'HP' AND fail_station = 'PRETEST'
                                                  THEN 'T_1'
                                              WHEN customer = 'HP' AND fail_station = 'POST RUNIN'
                                                  THEN 'T_2'
                                              WHEN customer = 'LENOVO' AND fail_station = 'Testing'
                                                  THEN 'T_3'
                                              ELSE 'T_4'
                                              END    customer_mark,
                                              nvl(sum(total_count),
                                                  0) ng_qty
                                          FROM
                                              fpyRepairStationDay
                                          WHERE level_code = 'L10' AND site_code = 'WH'
                                          GROUP BY
                                              site_code,
                                              level_code,
                                              cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER),
                                              customer,
                                              fail_station
                                      )t
                                  WHERE
                                      t.customer_mark <> 'T_4'
                              ) temp
                         pivot   ( SUM (ng_qty) FOR customer_mark IN ('T_1', 'T_2', 'T_3')  )
                         GROUP BY
                         site_code,
                         level_code,
                         month_id
                     ) t3
                         ON t3.site_code = t2.site_code
                            AND t3.level_code = t2.level_code
                            AND t3.month_id = t2.month_id
             ) t5
         GROUP BY
             t5.site_code,
             t5.level_code,
             t5.month_id
     ) t1
