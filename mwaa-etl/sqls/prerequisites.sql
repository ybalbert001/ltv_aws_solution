--历史raw数据
create table ltv_order_existed_raw DISTKEY(date) SORTKEY(order_time) as 
select id, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, 
dateadd(m, cast(60*24*random() as int), date::DATE) as order_time 
from dev.ecomm_data.googleecomm where date::DATE <= '2013-07-01'::DATE;


--未来要流式注入的数据
create table ltv_order_ingesting_raw DISTKEY(date) SORTKEY(order_time) as 
select id, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, 
dateadd(m, cast(60*24*random() as int), date::DATE) as order_time from dev.ecomm_data.googleecomm 
where date::DATE > '2013-07-01'::DATE;

--unload到s3
unload ('select * from ltv_order_ingesting_raw order by order_time asc')
to 's3://ltv-poc/ltv-ingestion-data/order_' 
iam_role 'arn:aws:iam::106839800180:role/redshift_kds_ingest_ml'
delimiter ','
parallel off;

--流式注入的物化视图
SET enable_case_sensitive_identifier to TRUE;
CREATE MATERIALIZED VIEW ltv_order_ingesting_mv DISTKEY(11) sortkey(1) AUTO REFRESH YES AS
SELECT 
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'id', 'S', true)::bigint as id,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'chain', 'S', true)::bigint as chain,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'dept', 'S', true)::bigint as dept,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'category', 'S', true)::bigint as category,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'company', 'S', true)::bigint as company,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'brand', 'S', true)::bigint as brand,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'productsize', 'S', true)::FLOAT8 as productsize,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'productmeasure', 'S', true)::varchar as productmeasure,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'purchasequantity', 'S', true)::bigint as purchasequantity,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'purchaseamount', 'S', true)::FLOAT8 as purchaseamount,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'date', 'S', true)::varchar as date,
json_extract_path_text(from_varbyte(kinesis_data,'utf-8'), 'dynamodb', 'NewImage', 'order_time', 'S', true)::varchar as order_time
FROM "kds"."ecomm-order-kds"
WHERE LENGTH(kinesis_data) < 65355 AND is_utf8(kinesis_data) AND is_valid_json(from_varbyte(kinesis_data, 'utf-8'))
