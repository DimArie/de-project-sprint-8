import os
import sys
import pytz
import math

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find() 

from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark import StorageLevel


studentusername = 'dimarieisr'

def fill_zone_mart(user_geo_mart, geo_events, msg_geo_tmp, zones_mart, spark):
    
    # По условию задачи 3
    # "Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя."
    # Я делаю вывод что отсюда можно взять пользователей региона по act_city
    df_geo_mart = spark.read.parquet(user_geo_mart)\
    .selectExpr("user_id", "act_city as city")\
    .sample(withReplacement=False, fraction=0.1, seed=20)\
    .persist(StorageLevel.DISK_ONLY)
    
    # log_df(df_geo_mart)
#     #вытаскиваем только нужный период. 
#     pathes =input_event_paths(start_date, depth, geo_events)
    
    df = spark.read\
    .option("basePath", geo_events)\
    .parquet(geo_events)\
       .selectExpr(
        "event.message_id AS message_id",
        "CASE WHEN event_type = 'subscription' THEN event.user " +
        "WHEN event_type = 'reaction' THEN event.reaction_from " +
        "ELSE event.message_from END AS user_id",  # Общее имя колонки для всех случаев
        "event_type",
        "date",
        "date_sub(date, dayofweek(date) - 1) AS week",  # Первый день недели
        "trunc(date, 'MM') AS month"  # Первый день месяца    
    )\
    .join(df_geo_mart, "user_id", how="inner")\
    .sample(withReplacement=False, fraction=0.1, seed=20).persist(StorageLevel.DISK_ONLY)
       
    # log_df(df)
    
    # Todo  тоже надо город добавить
    df_geo_msg_first = spark.read.parquet(msg_geo_tmp)\
    .join(df_geo_mart,["user_id", "city"], how="inner")\
    .groupBy("city","user_id").agg(F.min("date").alias("reg_date"))\
    .withColumn("week", F.expr("date_sub(reg_date, dayofweek(reg_date) - 1)")) \
    .withColumn("month", F.trunc(F.col("reg_date"), "MM"))\
    .select("city", "user_id","week", "month")\
    .groupBy("city","week", "month")\
    .agg(F.countDistinct("user_id").alias("user"))\
    .persist(StorageLevel.DISK_ONLY)

    
    df_pivot_week = df.groupBy("city", "week", "month") \
    .pivot("event_type") \
    .agg(F.count("*").alias("event_count")) \
    .withColumnRenamed("subscription", "week_subscription")\
    .withColumnRenamed("reaction", "week_reaction")\
    .withColumnRenamed("message", "week_message")\
    .persist(StorageLevel.DISK_ONLY)
    
    # print("df_pivot_week")
    # log_df(df_pivot_week) 
    
    df_pivot_month = df.groupBy("city", "month") \
    .pivot("event_type") \
    .agg(F.count("*").alias("event_count")) \
    .withColumnRenamed("subscription", "month_subscription")\
    .withColumnRenamed("reaction", "month_reaction")\
    .withColumnRenamed("message", "month_message")\
    .persist(StorageLevel.DISK_ONLY)
    
    # print("df_pivot_month")
    # log_df(df_pivot_month) 
    
    # объединяем данные. + добавляем город 
    res = df_pivot_week\
    .join(df_geo_msg_first.select("week", "city", "user"), ["week", "city"], how="left")\
    .join(df_pivot_month, ["month", "city"], how="left")\
    .join(df_geo_msg_first.groupBy("month", "city").agg(F.sum("user").alias("month_user")), ["month", "city"], how="left")\
    .withColumnRenamed("user", "week_user")
    
    
    # ToDo: сохраняем выборку куда-то в analytics
#     log_df(res) 
    res.write.mode("overwrite").parquet(zones_mart)
       

def main():
 
    user_geo_mart = sys.argv[1]
    geo_events = sys.argv[2]
    msg_geo_tmp = sys.argv[3]
    zones_mart = sys.argv[4]
    
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"prj-{studentusername}-fill_days_in_town") \
                        .getOrCreate()
 
    spark.sparkContext.setCheckpointDir(f"hdfs:///user/{studentusername}/temp")

    fill_zone_mart(user_geo_mart, geo_events, msg_geo_tmp, zones_mart, spark)

if __name__ == "__main__":
    main()

        