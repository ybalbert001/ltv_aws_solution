
insert into ltv_offline_feature_midd_lag
(
select id, date, lag(date, 1) over (partition by id order by date asc) as previous_date, '{{ ds }}' as dt 
from 
    (
        select id, date from ltv_order_existed_raw 
        where date::DATE <= ('{{ ds }}'::DATE - INTERVAL '365 day') 
        group by id, date
    ) t
);

insert into ltv_offline_feature_1
(
with stat_overall as (
    select id, 
    datediff(day, min(date::DATE), ('{{ ds }}'::DATE - INTERVAL '365 day')) as historical_day_cnt,
    count(1) as historical_order_cnt, 
    sum(purchaseamount) as historical_order_amount,
    avg(purchaseamount) as historical_agv_amount_per_order,
    stddev(purchaseamount) as historical_std_amount_per_order,
    max(purchaseamount) as historical_max_amount_per_order 
from ltv_order_existed_raw where date::DATE <= ('{{ ds }}'::DATE - INTERVAL '365 day') group by id 
),
stat_by_orderday as (
    select id, 
    count(1) as historical_orderday_cnt, 
    avg(order_cnt) as historical_agv_order_per_orderday, 
    stddev(order_cnt) as historical_std_order_per_orderday,
    avg(amount) as historical_agv_amount_per_orderday, 
    stddev(amount) as historical_std_amount_per_orderday,
    max(amount) as historical_max_amount_per_day 
from (
select id, date, count(1) as order_cnt, sum(purchaseamount) as amount 
from ltv_order_existed_raw where date::DATE <= ('{{ ds }}'::DATE - INTERVAL '365 day') group by id, date 
) t group by id
),
purchase_day_interval as (
    select id, avg(dayInterval) as avg_day_interval, stddev(dayInterval) as std_day_interval from
(
select id, datediff('day', previous_date::DATE, date::DATE) as dayInterval 
from ltv_offline_feature_midd_lag 
where dt = '{{ ds }}' and 
date::DATE <= ('{{ ds }}'::DATE - INTERVAL '365 day') and 
previous_date is not NULL
) t group by id
)
select a.id, historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, b.historical_orderday_cnt,
b.historical_agv_order_per_orderday, b.historical_std_order_per_orderday, b.historical_agv_amount_per_orderday,
b.historical_std_amount_per_orderday, b.historical_max_amount_per_day, c.avg_day_interval, c.std_day_interval, '{{ ds }}' as dt 
from stat_overall a 
left join stat_by_orderday b 
on a.id = b.id 
left join purchase_day_interval c 
on a.id = c.id
);




