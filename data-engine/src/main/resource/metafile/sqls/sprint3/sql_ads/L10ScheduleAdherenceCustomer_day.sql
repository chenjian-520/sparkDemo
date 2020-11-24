select
uuid() id,
d.site_code site_code,
d.level_code level_code,
d.work_dt work_date,
d.customer customer_code,
output_qty output_qty_actual,
schedule_qty work_order_qty,
-- cast(
--     get_aim_target_by_key(
--     concat_ws("=",'D',site_code,level_code,'all', 'all', 'all', 'all'),12
--   ) AS INTEGER) work_order_qty,
d.SA_Line schedule_adherence,
unix_timestamp() etl_time
from
(
select
a.site_code site_code,
a.level_code level_code,
a.factory_code factory_code,
a.process_code process_code,
a.customer customer,
a.work_dt work_dt,
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
    customer,
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
        work_dt,
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
    customer,
    work_dt
  ) as a
left join
  (
    select
    site_code,
    level_code,
    work_dt,
    upper(customer) customer,
    sum(schedule_qty) schedule_qty
    from
    (
      select
      site_code,
      level_code,
      factory_code,
      process_code,
      area_code,
      line_code,
      machine_id,
      work_dt,
      work_shift,
      customer,
      key,
      schedule_qty,
      update_dt,
      update_by,
      data_from
      from
      productionPlanView
      where level_code = 'L10' and site_code = 'WH'
    )
    group by
    site_code,
    level_code,
    work_dt,
    customer
  ) as b
on a.site_code=b.site_code
    AND a.level_code = b.level_code
--     AND a.factory_code = b.factory_code
--     AND a.process_code = b.process_code
    AND a.customer = b.customer
    AND a.work_dt = b.work_dt
) as d
where d.work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')
