import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}


studentusername = 'dimarieisr'


base_path = f"/user/{studentusername}/data/events"
event_geo_path = f"/user/{studentusername}/data/events"


stg_geo_ref_path = f"/user/{studentusername}/temp/geo/geo.csv"
masterdata_geo_events = "/user/master/data/geo/events"

geo_ref_path = f"/user/{studentusername}/data/ref/geo"
geo_events = f"/user/{studentusername}/data/geo/events"

# временнеая истоирия под сообщения с городами
msg_geo_tmp = f'/user/{studentusername}/temp/msg'


# куда сохраняем витрины
user_geo_mart = f"/user/{studentusername}/analytics/project/user_geo_mart"
zones_mart = f"/user/{studentusername}/analytics/project/zones_mart"
friends_mart = f"/user/{studentusername}/analytics/project/friends_mart"


dag_spark = DAG(
dag_id = "datalake-proj-zudov",
default_args=default_args,
schedule_interval="0 1 * * *",
)

starting_task = DummyOperator(task_id='starting_task', dag=dag_spark)

fill_geo_events_ods = SparkSubmitOperator(
task_id='fill_geo_events_ods',
dag=dag_spark,
application ='/lessons/fill_geo_events_ods.py' ,
conn_id= 'yarn_spark',
application_args = [masterdata_geo_events, geo_events],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 4,
executor_memory = '4g'
)

fill_geo_ods = SparkSubmitOperator(
task_id='fill_geo_ods',
dag=dag_spark,
application ='/lessons/fill_geo_ods.py' ,
conn_id= 'yarn_spark',
application_args = [stg_geo_ref_path, geo_ref_path],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

fill_msg_with_city = SparkSubmitOperator(
task_id='fill_msg_with_city',
dag=dag_spark,
application ='/lessons/fill_msg_with_city.py' ,
conn_id= 'yarn_spark',
application_args = [geo_ref_path, geo_events, msg_geo_tmp],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 4,
executor_memory = '8g'
)

fill_days_in_town = SparkSubmitOperator(
task_id='fill_days_in_town',
dag=dag_spark,
application ='/lessons/fill_days_in_town.py' ,
conn_id= 'yarn_spark',
application_args = [msg_geo_tmp, geo_ref_path, user_geo_mart],      
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 4,
executor_memory = '8g'
)

fill_zone_mart = SparkSubmitOperator(
task_id='fill_zone_mart',
dag=dag_spark,
application ='/lessons/fill_zone_mart.py' ,
conn_id= 'yarn_spark',
application_args = [user_geo_mart, geo_events, msg_geo_tmp, zones_mart], 
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 4,
executor_memory = '8g'
)


fill_friends_mart = SparkSubmitOperator(
task_id='fill_friends_mart',
dag=dag_spark,
application ='/lessons/fill_friends_mart.py' ,
conn_id= 'yarn_spark',
application_args = [user_geo_mart, geo_events, msg_geo_tmp, geo_ref_path, friends_mart], 
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 4,
executor_memory = '8g'
)


starting_task >> [fill_geo_events_ods, fill_geo_ods] >> fill_msg_with_city >> fill_days_in_town >> [fill_zone_mart, fill_friends_mart]

# events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84, user_interests_d7, user_interests_d28]   