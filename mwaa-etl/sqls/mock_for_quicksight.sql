CREATE VIEW ca_channel AS 
(
SELECT 0 as idx, 'yingyongbao' AS ca_channel
UNION ALL
SELECT 1 as idx, 'douyin' AS ca_channel
UNION ALL
SELECT 2 as idx, 'baidu' AS ca_channel
UNION ALL
SELECT 3 as idx, 'appstore' AS ca_channel
UNION ALL
SELECT 4 as idx, 'other' AS ca_channel
);

CREATE VIEW phone_brand AS (
SELECT 0 as idx, 'iphone' AS phone_brand
UNION ALL
SELECT 1 as idx, 'huawei' AS phone_brand
UNION ALL
SELECT 2 as idx, 'vivo' AS phone_brand
UNION ALL
SELECT 3 as idx, 'oppo' AS phone_brand
UNION ALL
SELECT 4 as idx, 'xiaomi' AS phone_brand
UNION ALL
SELECT 5 as idx, 'other' AS phone_brand
);

create view city_info as (
select 0 as idx, '河北' as province union all
select 1 as idx, '山西' as province union all
select 2 as idx, '辽宁' as province union all
select 3 as idx, '吉林' as province union all
select 4 as idx, '黑龙江' as province union all
select 5 as idx, '江苏' as province union all
select 6 as idx, '浙江' as province union all
select 7 as idx, '安徽' as province union all
select 8 as idx, '福建' as province union all
select 9 as idx, '江西' as province union all
select 10 as idx, '山东' as province union all
select 11  as idx, '湖北' as province union all
select 12  as idx, '湖南' as province union all
select 13  as idx, '广东' as province union all
select 14  as idx, '海南' as province union all
select 15  as idx, '四川' as province union all
select 16  as idx, '贵州' as province union all
select 17  as idx, '云南' as province union all
select 18  as idx, '陕西' as province union all
select 19  as idx, '甘肃' as province union all
select 20 as idx, '青海' as province union all
select 21 as idx, '内蒙古' as province union all
select 22 as idx, '广西' as province union all
select 23 as idx, '西藏' as province union all
select 24 as idx, '宁夏' as province union all
select 25 as idx, '新疆' as province union all
select 26 as idx, '北京' as province union all
select 27 as idx, '天津' as province union all
select 28 as idx, '上海' as province union all
select 29 as idx, '河南' as province union all
select 30 as idx, '重庆香港' as province union all
select 31 as idx, '澳门' as province
);

create table ltv_user_profile as 
select a.id, b.ca_channel, c.phone_brand, d.province from
(select id, floor(random()*5) as channel_id, floor(random()*6) as phone_idx, floor(random()*32) as province_idx 
from dev.ecomm_data.googleecomm group by id) a 
join ca_channel b 
on a.channel_id = b.idx 
join phone_brand c  
on a.phone_idx = c.idx 
join city_info d 
on a.province_idx = d.idx;

create view ltv_bi_source as 
select a.*, b.two_stage_predict_val as ltv365, b.slice from ltv_user_profile a 
join
(
select id, slice, EXP(case when classification_model_predict = 'onetime_buyer' then 6.0 else regression_model_predict end) as two_stage_predict_val from ltv_real_infer_result 
) b 
on a.id = b.id