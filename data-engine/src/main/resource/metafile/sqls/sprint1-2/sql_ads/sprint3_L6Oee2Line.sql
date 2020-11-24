select
sum(ttl_pass_station*smt_ct) as oee2_actual,
line_code,
work_dt
from
dpm_dws_production_partno_day
group by
line_code,
work_dt


