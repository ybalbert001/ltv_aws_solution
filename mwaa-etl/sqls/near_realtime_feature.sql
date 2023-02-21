-- '{{ ds }}' 为当天
CREATE OR REPLACE VIEW ltv_inference_real_input_view 
AS (
select id, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date,
substring(order_time,1,19)::datetime as order_time from ltv_order_ingesting_mv where date = '{{ ds }}' and substring(order_time,1,19)::DATETIME <= '{{ ts }}'::DATETIME 
);

insert into ltv_real_infer_feature
(
with stat_today as (
    select id, 
    count(1) as today_order_cnt, 
    sum(purchaseamount) as today_order_amount,
    avg(purchaseamount) as today_agv_amount_per_order,
    stddev(purchaseamount) as today_std_amount_per_order,
    max(purchaseamount) as today_max_amount_per_order,
    max(order_time) as latest_update_time  
    from ltv_inference_real_input_view 
    group by id
)
select a.id, historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, ltv7, ltv14, ltv30, 
today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order, 
today_max_amount_per_order, latest_update_time, '{{ ts }}' as slice  
from stat_today a 
join (select * from ltv_offline_feature_1 where dt = '{{ yesterday_ds }}') b 
on a.id = b.id 
left join (select * from ltv_offline_feature_2 where dt = '{{ yesterday_ds }}') c 
on a.id = c.id
);