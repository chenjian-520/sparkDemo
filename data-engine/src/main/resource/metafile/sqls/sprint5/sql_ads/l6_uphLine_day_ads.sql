select
  id,
  site_code,
  level_code,
  factory_code,
  if(process_code='all','SMT/PTH',process_code) process_code,
  line_code,
  work_date,
  output_qty_actual,
  uph_actual,
  output_hours,
  etl_time
from
(
  select
    uuid()  id,
    site_code,
    level_code,
    if(nvl(factory_code,'all')='','all',nvl(factory_code,'all')) factory_code,
    if(nvl(process_code,'all')='','all',nvl(process_code,'all')) process_code,
    line_code,
    work_dt             work_date,
    sum(output_qty)     output_qty_actual,
    sum(output_qty)/24                                uph_actual,
    24                  output_hours,
    cast(
      get_aim_target_by_key(
      concat_ws("=",'D',site_code,level_code,factory_code, process_code, line_code, 'all'),17
    ) AS INTEGER) uph_target,
    cast(unix_timestamp() AS VARCHAR(32))   etl_time
  from
    (
      select
        area_code,
        customer,
        data_from,
        data_granularity,
        factory_code,
        level_code,
        LineTotranfView(line_code) line_code,
        normalized_output_qty,
        output_qty,
        part_no,
        platform,
        process_code,
        site_code,
        sku,
        update_by,
        update_dt,
        work_dt,
        work_shift,
        workorder_type
      from
        L6UphLineView
    )
  where work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')
  group by
    site_code,
    level_code,
    factory_code,
    process_code,
    line_code,
    work_dt
)