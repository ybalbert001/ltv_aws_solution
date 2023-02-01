### 介绍

1. /dynamodb_writer 主要用于模拟数据埋点，发送数据到dynamodb 
2. /mwaa-etl
	+ 以redshift为etl数据处理的核心
	  - 以上游的dynamodb或者aurora 以实时注入等zero-etl的方式 作为数据输入
	  - 通过 redshift-ml 进行自动的 模型训练和部署
	+ 以mwaa(托管airflow) 作为调度工具
	  - 进行 T+1 的模型更新
	  - 进行分钟级的 近实时的 结果推理 



