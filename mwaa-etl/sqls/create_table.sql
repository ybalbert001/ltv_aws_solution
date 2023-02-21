CREATE TABLE IF NOT EXISTS ltv_offline_feature_midd_lag (
    id bigint ENCODE az64,
    date character varying(16383) ENCODE raw,
    previous_date character varying(16383) ENCODE raw,
    dt character varying(16383) ENCODE raw distkey
)
DISTSTYLE KEY 
SORTKEY ( id );

CREATE TABLE IF NOT EXISTS ltv_offline_feature_1 (
    id bigint ENCODE az64,
    historical_day_cnt bigint ENCODE az64,
    historical_order_cnt bigint ENCODE az64,
    historical_order_amount double precision ENCODE raw,
    historical_agv_amount_per_order double precision ENCODE raw,
    historical_std_amount_per_order double precision ENCODE raw,
    historical_max_amount_per_order double precision ENCODE raw,
    historical_orderday_cnt bigint ENCODE az64,
    historical_agv_order_per_orderday bigint ENCODE az64,
    historical_std_order_per_orderday double precision ENCODE raw,
    historical_agv_amount_per_orderday double precision ENCODE raw,
    historical_std_amount_per_orderday double precision ENCODE raw,
    historical_max_amount_per_day double precision ENCODE raw,
    avg_day_interval bigint ENCODE az64,
    std_day_interval double precision ENCODE raw,
    dt character varying(16383) ENCODE lzo distkey
)
DISTSTYLE KEY 
SORTKEY ( id ); 

CREATE TABLE IF NOT EXISTS ltv_offline_feature_2 (
    id bigint ENCODE az64,
    monthly_day_cnt bigint ENCODE az64,
    monthly_order_cnt bigint ENCODE az64,
    monthly_order_amount double precision ENCODE raw,
    monthly_agv_amount_per_order double precision ENCODE raw,
    monthly_std_amount_per_order double precision ENCODE raw,
    monthly_max_amount_per_order double precision ENCODE raw,
    monthly_orderday_cnt bigint ENCODE az64,
    monthly_agv_order_per_orderday bigint ENCODE az64,
    monthly_std_order_per_orderday double precision ENCODE raw,
    monthly_agv_amount_per_orderday double precision ENCODE raw,
    monthly_std_amount_per_orderday double precision ENCODE raw,
    monthly_max_amount_per_day double precision ENCODE raw,
    ltv7 double precision ENCODE raw,
    ltv14 double precision ENCODE raw,
    ltv30 double precision ENCODE raw,
    dt character varying(16383) ENCODE lzo distkey
)
DISTSTYLE KEY 
SORTKEY ( id ); 

CREATE TABLE IF NOT EXISTS ltv_realtime_feature (
    id bigint ENCODE az64,
    today_order_cnt bigint ENCODE az64,
    today_order_amount double precision ENCODE raw,
    today_agv_amount_per_order double precision ENCODE raw,
    today_std_amount_per_order double precision ENCODE raw,
    today_max_amount_per_order double precision ENCODE raw,
    dt character varying(16383) ENCODE lzo distkey
)
DISTSTYLE KEY 
SORTKEY ( id ); 

CREATE TABLE IF NOT EXISTS ltv_order_dataset_label (
    id bigint ENCODE az64,
    ltv365 double precision ENCODE raw,
    dt character varying(16383) ENCODE lzo distkey
)
DISTSTYLE KEY 
SORTKEY ( id );

CREATE TABLE IF NOT EXISTS ltv_model_dataset (
    id bigint ENCODE az64,
    historical_day_cnt bigint ENCODE az64,
    historical_order_cnt bigint ENCODE az64,
    historical_order_amount double precision ENCODE raw,
    historical_agv_amount_per_order double precision ENCODE raw,
    historical_std_amount_per_order double precision ENCODE raw,
    historical_max_amount_per_order double precision ENCODE raw,
    historical_orderday_cnt bigint ENCODE az64,
    historical_agv_order_per_orderday bigint ENCODE az64,
    historical_std_order_per_orderday double precision ENCODE raw,
    historical_agv_amount_per_orderday double precision ENCODE raw,
    historical_std_amount_per_orderday double precision ENCODE raw,
    historical_max_amount_per_day double precision ENCODE raw,
    avg_day_interval bigint ENCODE az64,
    std_day_interval double precision ENCODE raw,
    monthly_day_cnt bigint ENCODE az64,
    monthly_order_cnt bigint ENCODE az64,
    monthly_order_amount double precision ENCODE raw,
    monthly_agv_amount_per_order double precision ENCODE raw,
    monthly_std_amount_per_order double precision ENCODE raw,
    monthly_max_amount_per_order double precision ENCODE raw,
    monthly_orderday_cnt bigint ENCODE az64,
    monthly_agv_order_per_orderday bigint ENCODE az64,
    monthly_std_order_per_orderday double precision ENCODE raw,
    monthly_agv_amount_per_orderday double precision ENCODE raw,
    monthly_std_amount_per_orderday double precision ENCODE raw,
    monthly_max_amount_per_day double precision ENCODE raw,
    ltv7 double precision ENCODE raw,
    ltv14 double precision ENCODE raw,
    ltv30 double precision ENCODE raw,
    today_order_cnt bigint ENCODE az64,
    today_order_amount double precision ENCODE raw,
    today_agv_amount_per_order double precision ENCODE raw,
    today_std_amount_per_order double precision ENCODE raw,
    today_max_amount_per_order double precision ENCODE raw,
    label double precision ENCODE raw,
    dt character varying(16383) ENCODE raw distkey
)
DISTSTYLE KEY 
SORTKEY ( id );

CREATE TABLE IF NOT EXISTS ltv_model_evaluation (
    eval_target character varying(16383) ENCODE raw,
    metric_name character varying(16383) ENCODE raw,
    metric_value double precision ENCODE raw,
    dt character varying(16383) ENCODE raw distkey
)
DISTSTYLE KEY;

CREATE TABLE IF NOT EXISTS ltv_eval_result (
    id bigint ENCODE az64,
    classification_model_predict character varying(16383) ENCODE raw,
    regression_model_predict double precision ENCODE raw,
    label double precision ENCODE raw,
    dt character varying(16383) ENCODE raw distkey
)
DISTSTYLE KEY 
SORTKEY ( id );

CREATE TABLE IF NOT EXISTS ltv_real_infer_feature (
    id bigint ENCODE az64,
    historical_day_cnt int8,
    historical_order_cnt int8,
    historical_order_amount float8,
    historical_agv_amount_per_order float8,
    historical_std_amount_per_order float8,
    historical_max_amount_per_order float8,
    historical_orderday_cnt int8,
    historical_agv_order_per_orderday int8,
    historical_std_order_per_orderday float8,
    historical_agv_amount_per_orderday float8,
    historical_std_amount_per_orderday float8,
    historical_max_amount_per_day float8,
    avg_day_interval int8,
    std_day_interval float8,
    monthly_day_cnt int8,
    monthly_order_cnt int8,
    monthly_order_amount float8,
    monthly_agv_amount_per_order float8,
    monthly_std_amount_per_order float8,
    monthly_max_amount_per_order float8,
    monthly_orderday_cnt int8,
    monthly_agv_order_per_orderday int8,
    monthly_std_order_per_orderday float8,
    monthly_agv_amount_per_orderday float8,
    monthly_std_amount_per_orderday float8,
    monthly_max_amount_per_day float8,
    ltv7 float8,
    ltv14 float8,
    ltv30 float8,
    today_order_cnt int8, 
    today_order_amount float8, 
    today_agv_amount_per_order float8, 
    today_std_amount_per_order float8,
    today_max_amount_per_order float8,
    lastest_order_time timestamp without time zone,
    slice character varying(16383) ENCODE raw distkey
)
DISTSTYLE KEY 
SORTKEY ( id );


CREATE TABLE IF NOT EXISTS ltv_real_infer_result (
    id bigint ENCODE az64,
    classification_model_predict character varying(16383) ENCODE raw,
    regression_model_predict double precision ENCODE raw,
    slice character varying(16383) ENCODE raw distkey
)
DISTSTYLE KEY 
SORTKEY ( id );