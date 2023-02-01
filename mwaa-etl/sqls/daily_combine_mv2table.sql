insert into ltv_order_existed_raw
(
select * from ltv_order_ingesting_view where date = '{{ ds }}'
);