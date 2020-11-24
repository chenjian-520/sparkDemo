select
a.site_code site_code,
a.level_code level_code,
a.factory_code factory_code,
a.process_code process_code,
a.line_code line_code,
a.part_no part_no,
a.work_dt month_id,
a.output_qty output_qty,
b.schedule_qty schedule_qty,
(1-(b.schedule_qty-a.output_qty)/b.schedule_qty)*100 SA_Line
from
  (
    select
    site_code,
    level_code,
    factory_code,
    process_code,
    line_code,
    part_no,
    work_dt,
    sum(output_qty) output_qty
    from
      (
       select
       a.site_code site_code,
       a.level_code level_code,
       a.factory_code factory_code,
       a.process_code process_code,
       lineView.line line_code,
       a.part_no part_no,
       a.work_dt work_dt,
       a.output_qty output_qty,
       a.data_granularity data_granularity
       from
        (select
        site_code,
        level_code,
        factory_code,
        process_code,
        line_code,
        part_no,
        cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt,
        output_qty,
        data_granularity
        from
        l6SASiteLineView
        where data_granularity = 'line') as a
        left join
        lineView
        on
        a.line_code = lineView.line_XF
      )
    group by
    site_code,
    level_code,
    factory_code,
    process_code,
    line_code,
    part_no,work_dt
  ) as a
left join
  (
    select
    site_code,
    level_code,
    factory_code,
    process_code,
    area_code,
    line_code,
    machine_id,
    cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt,
    work_shift,
    customer,
    key,
    schedule_qty,
    update_dt,
    update_by,
    data_from
    from
    productionPlanView
    where level_code = 'L6' and site_code = 'WH'
  ) as b
on a.site_code=b.site_code
    AND a.level_code = b.level_code
    AND a.factory_code = b.factory_code
    AND a.process_code = b.process_code
    AND a.line_code = b.line_code
    AND a.part_no = b.key
    AND a.work_dt = b.work_dt

