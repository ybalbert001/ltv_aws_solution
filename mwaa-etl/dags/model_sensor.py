import boto3
import time

def check_model_status(model_id, cluster_name = 'redshift-cluster-4', db_name = 'dev', db_user='awsuser'):
	redshift_client = boto3.client("redshift-data")
	mid_result = redshift_client.execute_statement(
		ClusterIdentifier = cluster_name,
		Database = db_name,
		DbUser = db_user,
		Sql = 'show model {}'.format(model_id)
	)


	status_obj = redshift_client.describe_statement(Id = mid_result['Id'])
	while status_obj['Status'] != 'FINISHED':
		if status_obj['Status'] == 'FAILED':
			return False

		status_obj = redshift_client.describe_statement(Id = mid_result['Id'])
		time.sleep(1)

	result = redshift_client.get_statement_result(Id = mid_result['Id'])

	for arr in result['Records']:
		if arr[0]['stringValue'] == 'Model State':
			if arr[1]['stringValue'] == 'READY':
				return True

	return False

def check_both_model(dt):
	reg_model_id = "{}_{}".format('ltv_regression_auto_model', dt)
	bin_model_id = "{}_{}".format('ltv_binary_auto_model', dt)
	return check_model_status(reg_model_id) and check_model_status(bin_model_id)

# print(check_both_model('20130701'))