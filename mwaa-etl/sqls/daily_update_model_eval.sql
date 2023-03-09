insert into ltv_eval_result
(
select id, 
ml_fn_ltv_regression_predict_v1{{ ds_nodash }}(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order) as regression_model_predict_v1, 
ml_fn_ltv_regression_predict_v2{{ ds_nodash }}(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, 
ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,
today_max_amount_per_order) as regression_model_predict_v2, label, '{{ ds }}' as dt from ltv_model_testset_view
);

insert into ltv_model_evaluation 
(
with predict_regression_eval as (
    select 
    avg(abs(predict_val_v1 - label)) as v1_mae, 
    avg(abs(predict_val_v1 - label)/predict_val_v1) as v1_mape, 
    avg(abs(predict_val_v2 - label)) as v2_mae, 
    avg(abs(predict_val_v2 - label)/predict_val_v2) as v2_mape 
    from 
    (
        select regression_model_predict_v1 as predict_val_v1, EXP(regression_model_predict_v2) as predict_val_v2, label from ltv_eval_result where dt = '{{ ds }}'
    ) t
)
select 'All' as eval_target, 'v1_mae' as metric_name, v1_mae as metric_val, '{{ ds }}' as dt from predict_regression_eval 
union all 
select 'All' as eval_target, 'v1_mape' as metric_name, v1_mape as metric_val, '{{ ds }}' as dt from predict_regression_eval 
union all 
select 'All' as eval_target, 'v2_mae' as metric_name, v2_mae as metric_val, '{{ ds }}' as dt from predict_regression_eval 
union all 
select 'All' as eval_target, 'v2_mape' as metric_name, v2_mape as metric_val, '{{ ds }}' as dt from predict_regression_eval 
);