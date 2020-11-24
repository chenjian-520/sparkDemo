select
a.site_code site_code,
a.level_code level_code,
a.factory_code factory_code,
a.process_code process_code,
a.line_code line_code,
a.customer customer,
a.platform platform,
a.work_dt year_id,
a.output_qty output_qty,
b.schedule_qty schedule_qty,
(1-abs(b.schedule_qty-a.output_qty)/b.schedule_qty)*100 SA_Line
from
  (
    select
    site_code,
    level_code,
    factory_code,
    process_code,
    line_code,
    customer,
    platform,
    work_dt,
    sum(output_qty) output_qty
    from
      (
        select
        site_code,
        level_code,
        factory_code,
        process_code,
        line_code,
        customer,
        platform,
        year(work_dt) work_dt,
        output_qty,
        data_granularity
        from
        l10SASiteLineView
        where data_granularity = 'process' and  process_code = 'ASSEMBLY1'
      )
    group by
    site_code,
    level_code,
    factory_code,
    process_code,
    line_code,
    customer,
    platform,
    work_dt
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
    year(work_dt) work_dt,
    work_shift,
    upper(customer) customer,
    key,
    schedule_qty,
    update_dt,
    update_by,
    data_from
    from
    productionPlanView
    where level_code = 'L10' and site_code = 'WH'
  ) as b
on a.site_code=b.site_code
    AND a.level_code = b.level_code
--     AND a.factory_code = b.factory_code
--     AND a.process_code = b.process_code
    AND a.line_code = b.line_code
    AND a.customer = b.customer
    AND a.platform = b.key
    AND a.work_dt = b.work_dt

