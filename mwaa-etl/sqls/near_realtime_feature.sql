-- '{{ ds }}' 为当天
CREATE OR REPLACE VIEW ltv_inference_real_input_view 
AS (
select * from ltv_order_ingesting_view where date = '{{ ds }}' and order_time <= '{{ ts }}'::DATETIME 
union all 
select * from ltv_order_existed_raw where date::DATE > ('{{ ds }}'::DATE - INTERVAL '29 day')
);

insert into ltv_real_infer_feature
(
with stat_overall as (
    select id, 
    datediff(day, min(date::DATE), ('{{ ds }}'::DATE - INTERVAL '365 day')) as monthly_day_cnt,
    count(1) as monthly_order_cnt, 
    sum(purchaseamount) as monthly_order_amount,
    avg(purchaseamount) as monthly_agv_amount_per_order,
    stddev(purchaseamount) as monthly_std_amount_per_order,
    max(purchaseamount) as monthly_max_amount_per_order,
    max(order_time) as latest_update_time
    from ltv_inference_real_input_view 
    group by id
),
stat_by_orderday as (
    select id, 
    count(1) as monthly_orderday_cnt, 
    avg(order_cnt) as monthly_agv_order_per_orderday, 
    stddev(order_cnt) as monthly_std_order_per_orderday,
    avg(amount) as monthly_agv_amount_per_orderday, 
    stddev(amount) as monthly_std_amount_per_orderday,
    max(amount) as monthly_max_amount_per_day 
    from (
        select id, date, count(1) as order_cnt, sum(purchaseamount) as amount from ltv_inference_real_input_view 
        group by id, date
    ) t group by id
),
ltv7 as (
    select id, sum(purchaseamount) as ltv7 
    from ltv_inference_real_input_view 
    where purchaseamount >0.0 and date::DATE > ('{{ ds }}'::DATE - INTERVAL '29 day') 
    and date::DATE <= ('{{ ds }}'::DATE - INTERVAL '22 day') group by id
),
ltv14 as (
    select id, sum(purchaseamount) as ltv14 
    from ltv_inference_real_input_view 
    where purchaseamount >0.0 and date::DATE > ('{{ ds }}'::DATE - INTERVAL '29 day') 
    and date::DATE <= ('{{ ds }}'::DATE - INTERVAL '15 day') group by id
),
ltv30 as (
    select id, sum(purchaseamount) as ltv30 
    from ltv_inference_real_input_view 
    where purchaseamount >0.0 group by id
)
select a.id, historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, b.monthly_orderday_cnt,
b.monthly_agv_order_per_orderday, b.monthly_std_order_per_orderday, b.monthly_agv_amount_per_orderday,
b.monthly_std_amount_per_orderday, b.monthly_max_amount_per_day, c.ltv7, d.ltv14, e.ltv30, a.latest_update_time, '{{ ts }}' as slice  
from stat_overall a 
join (select * from ltv_offline_feature where dt::DATE = ('{{ ds }}'::DATE - INTERVAL '1 day')) aa 
on a.id = aa.id 
left join stat_by_orderday b 
on a.id = b.id 
left join ltv7 c 
on a.id = c.id 
left join ltv14 d 
on a.id = d.id 
left join ltv30 e 
on a.id = e.id
);