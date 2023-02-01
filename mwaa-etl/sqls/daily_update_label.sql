insert into ltv_order_dataset_label
(
select id, sum(purchaseamount) as ltv365, '{{ ds }}' as dt from ltv_order_existed_raw 
where date::DATE > ('{{ ds }}'::DATE - INTERVAL '365 day') and date <= '{{ ds }}' 
group by id
);