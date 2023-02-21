insert into ltv_model_dataset
(
select a1.id, historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order, c.ltv365 as label, '{{ ds }}' as dt 
from (select * from ltv_offline_feature_1 where dt = '{{ ds }}') a1 
join (select * from ltv_order_dataset_label where dt = '{{ ds }}' ) c 
on a1.id = c.id 
left join (select * from ltv_offline_feature_2 where dt = '{{ ds }}' ) a2 
on a1.id = a2.id 
left join (select * from ltv_realtime_feature where dt = '{{ ds }}' ) b 
on a1.id = b.id 
);

drop view IF EXISTS ltv_model_testset_view;
create view ltv_model_testset_view as 
SELECT * FROM ltv_model_dataset where dt = '{{ ds }}' and LN(label) <6.0 and mod(abs(fnv_hash(id)),10) < 2 
union all 
SELECT * FROM ltv_model_dataset where dt = '{{ ds }}' and LN(label) >= 6.0 and mod(abs(fnv_hash(id)),200) = 1;

drop view IF EXISTS ltv_model_trainset_view;
create view ltv_model_trainset_view as 
SELECT * FROM ltv_model_dataset where dt = '{{ ds }}' and LN(label) <6.0 and mod(abs(fnv_hash(id)),10) >=5 
union all 
SELECT * FROM ltv_model_dataset where dt = '{{ ds }}' and LN(label) >= 6.0 and mod(abs(fnv_hash(id)),100) != 1;
