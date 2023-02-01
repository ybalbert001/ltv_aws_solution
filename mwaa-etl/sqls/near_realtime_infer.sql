insert into ltv_realtime_result
(
select id, 
ml_fn_ltv_binary_predict(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30) as classification_model_predict, 
ml_fn_ltv_regression_predict(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30) as regression_model_predict, '{{ ts }}' as slice from ltv_inference_feature_view
where latest_update_time > dateadd(m, -5, '{{ ts }}'::DATETIME) 
and latest_update_time<= '{{ ts }}'::DATETIME
);