select
uuid() id,
site_code,
level_code,
work_dt work_date,
output_qty output_qty_actual,
schedule_qty work_order_qty,
cast(
    get_aim_target_by_key(
    concat_ws("=",'D',site_code,level_code,'all', 'all', 'all', 'all'),12
  ) AS FLOAT )*100 schedule_adherence_target,
SA_Level schedule_adherence,
unix_timestamp() etl_time
from
(
select
a.site_code site_code,
a.level_code level_code,
a.work_dt work_dt,
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
        work_dt,
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
    productionPlanView
    where level_code = 'L6'
    group by
    site_code,
    level_code,
    work_dt
  ) as b
on a.site_code=b.site_code
    AND a.level_code = b.level_code
    AND a.work_dt = b.work_dt
) where work_dt = from_unixtime((unix_timestamp()-(60*60*24)), 'yyyy-MM-dd')









-- cast(
--     get_aim_target_by_key(
--     concat_ws('=','D',site_code,if(level_code='','all',level_code),if(factory_code='','all',factory_code),if(process_code='','all',process_code), 'all', 'all'),
--     case when emp_humman_resource_code='DL1V' then 8
--     when emp_humman_resource_code='DL2V' then 9
--     when emp_humman_resource_code='DL2F' then 10
--      when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11
--     end
--   ) AS INTEGER) headcount_target

-- cast(
--     get_aim_target_by_key(
--     concat_ws('=','D',site_code,level_code,'all','all', 'all', 'all'),
--     case when emp_humman_resource_code='DL1V' then 8
--     when emp_humman_resource_code='DL2V' then 9
--     when emp_humman_resource_code='DL2F' then 10
--      when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11
--     end
--   ) AS INTEGER) headcount_target

-- cast(
--     get_aim_target_by_key(
--     concat_ws('=','D',site_code,level_code,factory_code,'all', 'all', 'all'),
--     case when emp_humman_resource_code='DL1V' then 8
--     when emp_humman_resource_code='DL2V' then 9
--     when emp_humman_resource_code='DL2F' then 10
--      when emp_humman_resource_code='IDL1F' or emp_humman_resource_code='IDL1V' or emp_humman_resource_code='IDL2F' or emp_humman_resource_code='IDL2V' then 11
--     end
--   ) AS INTEGER) headcount_target