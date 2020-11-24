select
LineTotranfView(a.line_code) line_code,
a.station_code station_code,
a.work_dt work_dt,
sum(a.fail_count) fail_count,
sum(a.total_count) total_count
FROM
(
select *
from dwsProductionPassStation
where
site_code='WH' and level_code = 'L6' and (customer='DELL'or customer = 'LENOVO' or customer = 'Lenovo_CODE' or customer = 'HP')
)
as a
group by
a.line_code,
a.station_code,
a.work_dt



