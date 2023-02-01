
insert into ltv_model_evaluation 
(
with predict_binary_eval as (
select 
count(case when LN(label) <6.0 then 1 else NULL end) as onetime_buyer_label_cnt,
count(case when LN(label) >=6.0 then 1 else NULL end) as repeat_buyer_label_cnt,
count(case when classification_model_predict = 'onetime_buyer' then 1 else NULL end) as onetime_buyer_predict_cnt,
count(case when classification_model_predict = 'repeated_buyer' then 1 else NULL end) as repeat_buyer_predict_cnt,
count(case when LN(label) <6.0 and classification_model_predict = 'onetime_buyer' then 1 else NULL end) as TN,
count(case when LN(label) <6.0 and classification_model_predict = 'repeated_buyer' then 1 else NULL end) as FN,
count(case when LN(label) >=6.0 and classification_model_predict = 'onetime_buyer' then 1 else NULL end) as FP,
count(case when LN(label) >=6.0 and classification_model_predict = 'repeated_buyer' then 1 else NULL end) as TP,
EXP(case when classification_model_predict = 'onetime_buyer' then 6.0 else regression_model_predict end) as two_stage_predict_val,
EXP(regression_model_predict) as one_stage_predict_val
from 
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
ltv7, ltv14, ltv30) as regression_model_predict, label from 
ltv_model_testset_view
) t
)
select 'onetime-buyer' as eval_target, 'precision' as metric_name, TN::Float / onetime_buyer_predict_cnt as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all
select 'onetime-buyer' as eval_target, 'recall' as metric_name, TN::Float / onetime_buyer_label_cnt as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all
select 'repeat-buyer' as eval_target, 'precision' as metric_name, TP::Float / repeat_buyer_predict_cnt as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all
select 'repeat-buyer' as eval_target, 'recall' as metric_name, TP::Float / repeat_buyer_label_cnt as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all
select 'All' as eval_target, 'accuracy' as metric_name,(TN+TP)::Float / (TN+TP+FN+FP) as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all
select 'All' as eval_target, 'onestage_mae' as metric_name, avg(abs(one_stage_predict_val - label)) as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all 
select 'All' as eval_target, 'onestage_mape' as metric_name, avg(abs(one_stage_predict_val - label)/one_stage_predict_val) as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all
select 'All' as eval_target, 'twostage_mae' as metric_name, avg(abs(two_stage_predict_val - label)) as metric_val, '{{ ds }}' as dt from predict_binary_eval
union all 
select 'All' as eval_target, 'twostage_mape' as metric_name, avg(abs(two_stage_predict_val - label)/two_stage_predict_val) as metric_val, '{{ ds }}' as dt from predict_binary_eval
);