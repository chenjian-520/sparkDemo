select
uuid() id,
site_code,
level_code,
month_id,
output_qty output_qty_actual,
schedule_qty work_order_qty,
cast(
    get_aim_target_by_key(
    concat_ws("=",'D',site_code,level_code,'all', 'all', 'all', 'all'),12
  ) AS FLOAT)*100 schedule_adherence_target,
SA_Level schedule_adherence,
unix_timestamp() etl_time
from
(
select
a.site_code site_code,
a.level_code level_code,
a.work_dt month_id,
a.output_qty output_qty,
b.schedule_qty schedule_qty,
(1-(b.schedule_qty-a.output_qty)/b.schedule_qty)*100 SA_Level
from
  (
    select
    site_code,
    level_code,
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
        part_no,
        cast(from_unixtime(to_unix_timestamp(work_dt, 'yyyy-MM-dd'), 'yyyyMM') AS INTEGER) work_dt,
        output_qty,
        data_granularity
        from
        l6SASiteLineView
        where data_granularity = 'line'
      )
    group by
    site_code,
    level_code,
    work_dt
  ) as a
left join
  (
    select
    site_code,
    level_code,
    work_dt,
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
    where level_code = 'L6'
    )
    group by
    site_code,
    level_code,
    work_dt
  ) as b
on a.site_code=b.site_code
    AND a.level_code = b.level_code
    AND a.work_dt = b.work_dt
) where month_id = cast(from_unixtime((unix_timestamp()-(60*60*24)), 'yyyyMM') AS INTEGER)

