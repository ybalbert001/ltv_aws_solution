DROP MODEL IF EXISTS ltv_regression_auto_model;
CREATE MODEL ltv_regression_auto_model 
FROM
    (
    SELECT historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,
historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,
historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,
historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,
monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,
monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,
monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,
monthly_std_amount_per_orderday, monthly_max_amount_per_day, ltv7, ltv14, ltv30, LN(label) as label 
from public.ltv_model_trainset_view where label > 0.0
    )
TARGET label FUNCTION ml_fn_ltv_regression_predict 
IAM_ROLE 'arn:aws:iam::106839800180:role/redshift_kds_ingest_ml' 
AUTO ON 
SETTINGS (
  S3_BUCKET 'ltv-poc'
);