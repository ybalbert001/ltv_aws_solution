insert into ltv_realtime_feature
(
with stat_today as (
    select id, 
    count(1) as today_order_cnt, 
    sum(purchaseamount) as today_order_amount,
    avg(purchaseamount) as today_agv_amount_per_order,
    stddev(purchaseamount) as today_std_amount_per_order,
    max(purchaseamount) as today_max_amount_per_order 
    from ltv_order_existed_raw where date::DATE = ('{{ ds }}'::DATE - INTERVAL '365 day') 
    group by id
)
select id, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order, '{{ ds }}' as dt 
from stat_today
);