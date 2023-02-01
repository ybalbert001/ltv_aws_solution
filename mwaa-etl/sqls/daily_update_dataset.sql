insert into ltv_model_dataset
(
select a.id, historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, c.ltv365 as label, '{{ ds }}' as dt 
from (select * from ltv_offline_feature where dt = '{{ ds }}') a 
left join (select * from ltv_realtime_feature where dt = '{{ ds }}' ) b 
on a.id = b.id 
left join (select * from ltv_order_dataset_label where dt = '{{ ds }}' ) c 
on a.id = c.id
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
