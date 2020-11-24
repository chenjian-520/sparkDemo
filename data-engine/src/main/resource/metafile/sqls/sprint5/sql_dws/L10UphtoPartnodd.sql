select
--   nvl(concat(
--           '20', ':',
--           concat(to_unix_timestamp(concat(a.work_dt, ' 00:00:00'), 'yyyy-MM-dd HH:mm:ss'), '000'), ':',
--           a.site_code, ':',
--           a.level_code, ':',
--           a.line_code, ':',
--           a.platform
--       ), 'N/A')                                             Rowkey,
a.site_code site_code,
a.level_code level_code,
a.factory_code factory_code,
a.process_code process_code,
a.line_code line_code,
a.platform platform,
a.part_no part_no,
a.customer customer,
a.work_dt work_dt,
a.output_qty output_qty,
cast (nvl(uphCt.cycle_time,'8') as VARCHAR(32)) ct,
a.area_code area_code
from
  (select
    site_code,
    level_code,
    factory_code,
    area_code,
    line_code,
    part_no,
    platform,
    station_code process_code,
    customer,
    work_dt,
    count(*) output_qty
  from OutputView
  group by
    site_code,
    level_code,
    factory_code,
    process_code,
    area_code,
    line_code,
    part_no,
    platform,
    customer,
    station_code,
    work_dt
  ) a
left join
  uphCt
on uphCt.site_code = a.site_code AND uphCt.level_code = a.level_code AND
   trim(lower(uphCt.line_code)) = trim(lower(a.line_code))  AND if(a.site_code = 'CQ' AND a.level_code = 'L10', uphCt.platform = a.part_no, uphCt.platform = a.platform)



