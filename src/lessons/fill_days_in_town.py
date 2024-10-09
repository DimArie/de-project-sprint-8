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


def fill_days_in_town(msg_geo_tmp, geo_ref_path, user_geo_mart, spark ):

    df = spark.read.parquet(msg_geo_tmp) # .sample(withReplacement=False, fraction=0.01)
    
    # Убираем дубликаты сообщений одного пользователя в одном городе за один и тот же день
    df_unique_days = df.groupBy("user_id", "city", "date").agg(F.max("datetime").alias("datetime"))


    # Используем лаг для нахождения предыдущей даты
    df_unique_days = df_unique_days.withColumn("previous_date", F.lag("date").\
       over(Window.partitionBy("user_id", "city").orderBy("date")))\
    .withColumn("day_diff", F.datediff("date", "previous_date"))\
    .withColumn("new_group_flag", F.when(F.col("day_diff") > 1, 1).otherwise(0))\
    .withColumn("group",
                F.sum("new_group_flag").\
                over(Window.partitionBy("user_id", "city").\
                     orderBy("date")))\
    .withColumn("sequence", F.row_number().over(Window.partitionBy("user_id", "city", "group").orderBy("date")))\
    .groupBy("user_id", "city", "group")\
    .agg(
        F.min("date").alias("first_date"), 
        F.max("date").alias("last_date"), 
        F.max("datetime").alias("last_action_datetime"), #добавляем дату последнего действия
        F.max("sequence").alias("days_in_town")
    )\
    .persist(StorageLevel.DISK_ONLY)

    # считаем сколько раз он был дома больше 27 дней подряд. Их в моем случае где-то 30 +- (что=то не так с данными или я дебил)
    #
    df_hs_27 = df_unique_days.where("days_in_town >= 27")\
    .withColumn("home_city", F.last("city").over(Window.partitionBy("user_id").orderBy("last_date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))\
    .select("user_id", "home_city")\
    .withColumnRenamed("user_id", "h_user_id")

#     
    
    # ищем тех, кто не менял города.
    df_unique_city = df_unique_days.groupBy("user_id").agg(
    F.countDistinct("city").alias("unique_city_count"),
    F.first("city").alias("unique_city")  
    ).where(F.col("unique_city_count") == 1).select("user_id", "unique_city")\
    .withColumnRenamed("user_id", "u_user_id")
    
#     log_df(df_unique_city.orderBy("u_user_id"))
    
    # напоследок, если оба условия не прокатили, то возмем город с наибольшим кол-вом послещений.
    # нам нужен будет этот город для последнего задания по друзьям. ограничим поиск друзей теми, кто живет в одном городе
    df_days_in_city = df_unique_days.groupBy("user_id", "city").agg(
    F.sum("days_in_town").alias("total_days_in_city")
    )\
    .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("total_days_in_city"))))\
    .where(F.col("rank") == 1).orderBy(F.asc("user_id"))\
    .withColumnRenamed("city", "max_total_days_city")\
    .select("user_id", "max_total_days_city")\
    .persist(StorageLevel.DISK_ONLY)
    
        
#     log_df(df_days_in_city)
    
    # получаем список актуальных городов и последнюю активность
    df_act_city = df_unique_days\
    .withColumn("act_city", F.last("city").over(Window.partitionBy("user_id").orderBy("last_date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))\
    .groupBy("user_id", "act_city")\
    .agg(F.min("last_action_datetime").alias("last_action_time"))\
    .persist(StorageLevel.DISK_ONLY)
#     .select()
#     log_df(df_act_city)
    
    # собираем список городов. Считаем разные города, не рядом. Если чел день не писал, то считаем это одним городом
    df_travel_city =  df_unique_days.withColumn("previous_city", F.lag("city")\
    .over(Window.partitionBy("user_id").orderBy("last_date")))\
    .withColumn("same_city", F.when(F.col("previous_city") == F.col("city"), 1).otherwise(0))\
    .where ("same_city == 0")\
    .select("user_id", "city", "last_date")\
    .groupBy("user_id")\
     .agg(
        F.count("*").alias("travel_count"),  # Количество городов
        F.collect_list(F.col("city")).alias("travel_array")  # Список городов
    )\
    .persist(StorageLevel.DISK_ONLY)
    
#     log_df(df_travel_city)
       
    geo = spark.read.parquet(geo_ref_path)
#     log_df(geo)
        
    # собираем данные по витрине (по крайней мере домашний город и актуальный город с посл. сообщением)  
    df_out = df_days_in_city\
    .join(df_hs_27, df_days_in_city["user_id"] == df_hs_27["h_user_id"], how='left')\
    .join(df_unique_city, df_days_in_city["user_id"] == df_unique_city["u_user_id"], how='left')\
    .join(df_act_city, "user_id", how='left')\
    .join(df_travel_city, "user_id", how='left')\
    .join(geo.select("timezone", "city"), geo["city"] == df_act_city["act_city"], how='inner' )\
    .withColumn("home_city", F.coalesce(
        F.col("home_city"),
        F.col("unique_city"),
        F.col("max_total_days_city")
    ))\
    .withColumn("local_time", F.from_utc_timestamp(F.col("last_action_time"), F.col("timezone")))\
    .drop("max_total_days_city", "u_user_id", "h_user_id", "max_total_days_city","unique_city","last_action_time","timezone", "city")\
    .persist(StorageLevel.DISK_ONLY)
    
#     log_df(df_out)
    # бьем по act_city.
    # Как будто кусок задания: "Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя."
    # подразумевает что городом пользователя будем считать его последний город пребывания
    df_out.write.option("header",True) \
    .partitionBy( "act_city") \
    .mode("overwrite") \
    .parquet(user_geo_mart) 
          

def main():
    msg_geo_tmp = sys.argv[1]
    geo_ref_path = sys.argv[2]
    user_geo_mart = sys.argv[3]
    
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"prj-{studentusername}-fill_days_in_town") \
                        .getOrCreate()
 
    spark.sparkContext.setCheckpointDir(f"hdfs:///user/{studentusername}/temp")

    fill_days_in_town(msg_geo_tmp, geo_ref_path, user_geo_mart, spark)

if __name__ == "__main__":
    main()

        