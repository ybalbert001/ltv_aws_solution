insert into ltv_order_existed_raw
(
select id, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date,
substring(order_time,1,19)::datetime from ltv_order_ingesting_mv where date = '{{ ds }}'
);


CREATE OR REPLACE VIEW ltv_real_infer_feature_view as 
(
with stat_today as (
    select id, 
    count(1) as today_order_cnt, 
    sum(purchaseamount) as today_order_amount,
    avg(purchaseamount) as today_agv_amount_per_order,
    stddev(purchaseamount) as today_std_amount_per_order,
    max(purchaseamount) as today_max_amount_per_order 
    from ltv_order_ingesting_mv where date = '{{ ds }}'
    group by id
)
select a1.id, historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order from 
(select * from ltv_offline_feature_1 where dt = '{{ yesterday_ds }}') a1 
left join (select * from ltv_offline_feature_2 where dt = '{{ yesterday_ds }}' ) a2 
on a1.id = a2.id 
left join stat_today b 
on a1.id = b.id 
)