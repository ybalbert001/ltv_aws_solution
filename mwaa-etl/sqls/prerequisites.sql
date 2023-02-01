--历史raw数据
create table ltv_order_existed_raw DISTKEY(date) SORTKEY(order_time) as 
select id, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, 
dateadd(m, cast(60*24*random() as int), date::DATE) as order_time 
from dev.googledata.googleecomm where date::DATE <= '2013-07-01'::DATE;


--未来要流式注入的数据
create table ltv_order_ingesting_raw DISTKEY(date) SORTKEY(order_time) as 
select id, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, 
dateadd(m, cast(60*24*random() as int), date::DATE) as order_time from dev.googledata.googleecomm 
where date::DATE > '2013-07-01'::DATE;

--unload到s3
unload ('select * from ltv_order_ingesting_raw order by order_time asc')
to 's3://ltv-poc/ltv-ingestion-data/order_' 
iam_role 'arn:aws:iam::106839800180:role/redshift_kds_ingest_ml'
delimiter ','
parallel off;

--流式注入的物化视图
CREATE MATERIALIZED VIEW ltv_order_ingesting_mv
AUTO REFRESH YES AS
SELECT JSON_PARSE(from_varbyte(kinesis_data, 'utf-8')) as json_data
FROM "kds"."ecomm-order-kds"
WHERE is_utf8(kinesis_data) AND is_valid_json(from_varbyte(kinesis_data, 'utf-8'));


--转换物化视图为schema对齐离线数据的view
SET enable_case_sensitive_identifier to TRUE;
create view ltv_order_ingesting_view as 
select json_data.dynamodb."NewImage".id."S"::bigint as id,
json_data.dynamodb."NewImage".chain."S"::bigint as chain,
json_data.dynamodb."NewImage".dept."S"::bigint as dept,
json_data.dynamodb."NewImage".category."S"::bigint as category,
json_data.dynamodb."NewImage".company."S"::bigint as company,
json_data.dynamodb."NewImage".brand."S"::bigint as brand,
json_data.dynamodb."NewImage".productsize."S"::FLOAT8 as productsize,
json_data.dynamodb."NewImage".productmeasure."S"::varchar as productmeasure,
json_data.dynamodb."NewImage".purchasequantity."S"::bigint as purchasequantity,
json_data.dynamodb."NewImage".purchaseamount."S"::FLOAT8 as purchaseamount,
json_data.dynamodb."NewImage".date."S"::varchar as date,
json_data.dynamodb."NewImage".order_time."S"::datetime as order_time
from ltv_order_ingesting_mv;