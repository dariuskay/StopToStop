from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta, date
from config import email, dns

default_args = {
    'owner': 'darius',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 1),
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('daily-spark-job', default_args=default_args, schedule_interval=timedelta(days=1))

today = date.today() - timedelta(days=2)
datestring = today.strftime('%Y/%m/%Y-%m-%d')
filestring = 'https://s3.amazonaws.com/nycbuspositions/' + datestring + '-bus-positions.csv.xz'

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
	task_id='wget-positions-file',
	bash_command='wget '+filestring + ' -P ~/data/',
	retries=3,
	dag=dag)

t2 = BashOperator(
	task_id='decompress-positions-file',
	bash_command='unxz '+'~/data/'+datestring[8:]+'-bus-positions.csv.xz',
	retries=3,
	dag=dag)

t3 = BashOperator(
	task_id='move-to-hdfs',
	bash_command='hdfs dfs -moveFromLocal ~/data/'+datestring[8:]+'-bus-positions.csv /data',
	dag=dag)

t4 = BashOperator(
	task_id='spark-submit-one',
	bash_command='spark-submit --master spark://'+dns+':7077 ~/sts2/script.py hdfs://'+dns+':8000/data/schedule_agglom.csv hdfs://'+dns+':8000/data/'+datestring[6:]+'-bus-positions.csv hdfs://'+dns+':8000/data/trips_agglom.csv',
	dag=dag)

t5 = BashOperator(
	task_id='spark-submit-two',
	bash_command='spark-submit --master spark://'+dns+':7077 ~/sts2/process.py',
	dag=dag)

t5.set_upstream(t4)
t4.set_upstream(t3)
t3.set_upstream(t2)
t2.set_upstream(t1)


