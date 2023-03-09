insert into ltv_real_infer_result
(
select id, 
ml_fn_ltv_regression_predict_v1_{{ yesterday_ds_nodash }}(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order) as regression_model_predict_v1, 
ml_fn_ltv_regression_predict_v2_{{ yesterday_ds_nodash }}(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order) as regression_model_predict_v2, '{{ ts }}' as slice from ltv_real_infer_feature 
where slice = '{{ ts }}' and lastest_order_time > dateadd(m, -5, '{{ ts }}'::DATETIME) 
and lastest_order_time <= '{{ ts }}'::DATETIME
);