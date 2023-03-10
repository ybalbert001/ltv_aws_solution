import boto3
import time
import json

redshift_client = boto3.client("redshift-data")

def lambda_handler(event, context):
    start = time.time()
    mid_result = redshift_client.execute_statement(
      ClusterIdentifier = event["cluster_id"],
      Database = event["db_name"],
      DbUser = event["db_user"],
      Sql = 'select {}(historical_day_cnt, historical_order_cnt, historical_order_amount, historical_agv_amount_per_order,historical_std_amount_per_order, historical_max_amount_per_order, historical_orderday_cnt,historical_agv_order_per_orderday, historical_std_order_per_orderday, historical_agv_amount_per_orderday,historical_std_amount_per_orderday, historical_max_amount_per_day, avg_day_interval, std_day_interval,monthly_day_cnt, monthly_order_cnt, monthly_order_amount, monthly_agv_amount_per_order,monthly_std_amount_per_order, monthly_max_amount_per_order, monthly_orderday_cnt,monthly_agv_order_per_orderday, monthly_std_order_per_orderday, monthly_agv_amount_per_orderday,monthly_std_amount_per_orderday, monthly_max_amount_per_day, ltv7, ltv14, ltv30, today_order_cnt, today_order_amount, today_agv_amount_per_order, today_std_amount_per_order,today_max_amount_per_order) from dev.public.ltv_real_infer_feature_view where id = {}'.format(event["model_name"], event["id"]))

    while(True):
      status_obj = redshift_client.describe_statement(Id = mid_result['Id'])
      if status_obj['Status'] == 'FINISHED':
        result = redshift_client.get_statement_result(Id = mid_result['Id'])
        user_ltv_type = result['Records'][0][0]['doubleValue']
        print("result: {}".format(user_ltv_type))
        break
      time.sleep(0.1)
      
    end = time.time()
    elapse = end - start
    result = {}
    result['elapse'] = elapse
    result['result'] = user_ltv_type
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
