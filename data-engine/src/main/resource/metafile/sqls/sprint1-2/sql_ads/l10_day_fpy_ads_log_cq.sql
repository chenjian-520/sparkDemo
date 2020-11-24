
     SELECT
            t4.site_code,
            t4.level_code,
            t4.work_dt,
            t4.customer,
            EXP(SUM(LOG(t4.station_ya))) AS customer_ya
     FROM
          (SELECT
                  t3.site_code,
                  t3.level_code,
                  t3.work_dt,
                  t3.customer,
                  t3.station_code,
                  (t3.input_qty - t3.ng_qty) / t3.input_qty AS station_ya
           FROM
                (SELECT
                        t1.site_code,
                        t1.level_code,
                        t1.work_dt,
                        t1.customer,
                        t1.station_code,
                        NVL(input_qty, 0) input_qty,
                        NVL(ng_qty, 0) ng_qty
                 FROM
                      (SELECT
                              site_code,
                              level_code,
                              work_dt,
                              station_code,
                              CASE
                                WHEN customer = 'HP' THEN customer
                                WHEN customer = 'DELL' THEN customer
                                WHEN customer = 'LENOVO' THEN customer
                                ELSE 'OTHER'
                                  END customer,
                              SUM(NVL(total_count, 0)) input_qty
                       FROM
                            fpyPassStationDay
                       WHERE
                           site_code = 'CQ' AND level_code='L10'
                       GROUP BY site_code , level_code , work_dt , customer , station_code) t1
                        LEFT JOIN (SELECT
                                          site_code,
                                          level_code,
                                          work_dt,
                                          fail_station,
                                          CASE
                                            WHEN customer = 'HP' THEN customer
                                            WHEN customer = 'DELL' THEN customer
                                            WHEN customer = 'LENOVO' THEN customer
                                            ELSE 'OTHER'
                                              END customer,
                                          NVL(SUM(total_count), 0) ng_qty
                                   FROM
                                        fpyRepairStationDay
                                   WHERE
                                       site_code = 'CQ' AND level_code='L10'
                                   GROUP BY site_code , level_code , work_dt , customer , fail_station) t2 ON t1.site_code = t2.site_code
                                                                                                                AND t1.level_code = t2.level_code
                                                                                                                AND t1.work_dt = t2.work_dt
                                                                                                                AND t1.customer = t2.customer
                                                                                                                AND t1.station_code = t2.fail_station) t3
           GROUP BY t3.site_code , t3.level_code , t3.work_dt , t3.customer , t3.station_code ,t3.input_qty ,t3.ng_qty) t4
     GROUP BY t4.site_code , t4.level_code , t4.work_dt , t4.customer

