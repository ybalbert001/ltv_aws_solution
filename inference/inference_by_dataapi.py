import boto3
import time
import json

id = 1728649856

redshift_client = boto3.client("redshift-data")

start = time.time()
mid_result = redshift_client.execute_statement(
  ClusterIdentifier = 'redshift-cluster-east-3',
  Database = 'dev',
  DbUser = 'awsuser',
  Sql = 'select ml_fn_ltv_binary_predict(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,monthly_std_amount_per_orderday, monthly_max_amount_per_day, ltv7, ltv14, ltv30) from dev.public.ltv_real_infer_feature_view where id = {}'.format(id))

while(True):
  status_obj = redshift_client.describe_statement(Id = mid_result['Id'])
  if status_obj['Status'] == 'FINISHED':
    result = redshift_client.get_statement_result(Id = mid_result['Id'])
    user_ltv_type = result['Records'][0][0]['stringValue']

    print("result: {}".format(user_ltv_type))
    end = time.time()
    elapse = end - start
    print("Processing time for {} is: {} seconds".format('redshift query', elapse))
    break
    
  time.sleep(0.1)