select
    count(*) "requests"
    , case
      when hour(access_time) in (0,1,2,3,4,5,6,7) then {threshold_one}
      when hour(access_time) in (8,9,10,11,12,13,14,15) then {threshold_two}
      when hour(access_time) in (16,17,18,19,20,21,22,23) then {threshold_three}
    else {threshold_three}
    end as "threshold"
    , case
      when hour(access_time) in (0,1,2,3,4,5,6,7) then 'threshold_one'
      when hour(access_time) in (8,9,10,11,12,13,14,15) then 'threshold_two'
      when hour(access_time) in (16,17,18,19,20,21,22,23) then 'threshold_three'
    else 'not_found'
    end as "threshold_bucket"
    , hour(now() AT TIME ZONE 'UTC') as "threshold_hour"
from
    bon_log_prod.access
where
    access_time > date_add('minute', - 60, NOW())
    and ds >= CAST(DATE(date_add('day', -1, NOW())) as VARCHAR) -- efficency, reduces the search scope
    and regexp_like(lower(request_path), '{ilike_url}') = true
    {account_id_condition}
    {domain_id_condition}
    {site_id_condition}
    {and_condition}
group by 2,3