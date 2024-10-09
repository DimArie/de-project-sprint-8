
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

def fill_friends_mart(user_geo_mart, geo_events, msg_geo_tmp, geo_ref_path, friends_mart, spark):
    

    # собираем выборку для всех пользователей с городами.
    # чтобы не уложить сервер, делаем допущение, что люди, 
    # живущие на расстоянии одного км друг от друга живут в одном городе
    df_geo_mart = spark.read.parquet(user_geo_mart)\
    .select("user_id", "home_city")\
    .sample(withReplacement=False, fraction=0.1, seed=20)\
    .persist(StorageLevel.DISK_ONLY)
      
    # получаем каналы (подписки)
    df = spark.read.parquet(geo_events)\
    .where("event_type='subscription'")\
    .selectExpr(
            "event.subscription_channel as channel",
            "event.user as user_id"
        )\
    .sample(withReplacement=False, fraction=0.1, seed=20)\
    .persist(StorageLevel.DISK_ONLY)

    # цепляем к данным по пользователям каналы.
    df = df_geo_mart.join(df, "user_id", how="inner")

    
    df1 = df.alias("df1")
    df2 = df.alias("df2")
    
    # считаем каждого с каждым но только один треугольник из матрицы. Считаем что если A рядом с B то нет смысла считать
    # что B рядом с A. Делаем это через user_id_1 > user_id_2
    # считаем челов из одного города, которые в одних каналах
    df_joined = df1.join(df2, 
    (F.col("df1.user_id") > F.col("df2.user_id")) & 
    (F.col("df1.channel") == F.col("df2.channel")) &
    (F.col("df1.home_city") == F.col("df2.home_city"))              
    , how="inner")\
    .select(
    F.col("df1.user_id").alias("user_id_1"),
    F.col("df2.user_id").alias("user_id_2"),
    F.col("df1.home_city").alias("city"),

    ).distinct()\
    .persist(StorageLevel.DISK_ONLY)
    
    # берем все личные сообщения
    df = spark.read.parquet(geo_events)\
    .where("event_type='message' AND event.message_to > 0")\
    .selectExpr(
            "event.message_from as user_id",  
            "event.message_to as contact_id",      
        )\
    .distinct()\
    .sample(withReplacement=False, fraction=0.001, seed=20).persist(StorageLevel.DISK_ONLY)

    # так как A пишет B а JOIN-ить 2 раза не хочется, то разворачиваем их наобород и присоединяем к массиву
    df_reversed = df.select(
        F.col("contact_id").alias("user_id"),
        F.col("user_id").alias("contact_id")
     )
    
    messages_df = df.union(df_reversed).distinct()
    

    # отсекаем личные сообщения 
    df_friends = df_joined.join(messages_df, 
       (
        (df_joined["user_id_1"] == messages_df["user_id"]) & 
        (df_joined["user_id_2"] == messages_df["contact_id"])
       ),
         how = "left_anti"
    )\
    .persist(StorageLevel.DISK_ONLY)
    
     
    # как определять что расстояния между людьми не превышает 1 км не написали (системных аналитиков нет).
    # возьмем данные, где мы определяли город и оттуда вытащим координаты.
    # поищем самые близкие координаты сообщений между людьми
    
    geo_tmp = spark.read.parquet(msg_geo_tmp)\
    .select (
        "user_id",
        "lat",
        "lon",
    )

    df_coord = df_friends.join(
        geo_tmp,
        (df_friends["user_id_1"] == geo_tmp["user_id"]),  
        how="inner"
    ).selectExpr(
        "user_id_1",
        "user_id_2",
        "city",
        "lat as user_id_1_lat",
        "lon as user_id_1_lon"
    ).join(
        geo_tmp,
        (df_friends["user_id_2"] == geo_tmp["user_id"]), 
        how="inner"
    ).selectExpr(
        "user_id_1",
        "user_id_2",
        "city",
        "user_id_1_lat",
        "user_id_1_lon",
        "lat as user_id_2_lat",
        "lon as user_id_2_lon"
    )\
    .persist(StorageLevel.DISK_ONLY)
#     получаем все события с координатами
    
    ### Тащим еще инфу по гео данным городов
    geo = spark.read.parquet(geo_ref_path)
                             
    # плохо звучит, но для оценки малых расстояний достаточно взять теорему Пифагора. 
    # Нет смысла морочиться с 3х этажными функциями
    # привет Меркаторовским проекциям на картах.
    
    df_coord = df_coord.withColumn(
    "distance", 
    F.sqrt(
        (F.col("user_id_1_lat") - F.col("user_id_2_lat")) ** 2 + 
        (F.col("user_id_1_lon") - F.col("user_id_2_lon")) ** 2
    ) * 111  # Умножение на 111 для преобразования в километры
    ).withColumn(
        "rank", 
        F.row_number().over(
            Window.partitionBy("user_id_1", "user_id_2").orderBy("distance")
        )
    ).where((F.col("rank") == 1) & (F.col("distance") <= 1.0))\
    .withColumn("processed_dttm", F.current_date())\
    .join(geo.select("timezone", "city"), "city", how='inner' )\
    .withColumn("local_time", F.from_utc_timestamp(F.current_timestamp(), F.col("timezone")))\
    .selectExpr(
    "user_id_1 AS user_left", 
    "user_id_2 AS user_right",
    "processed_dttm",
    "id zone_id",
    "local_time"
    )
    
    df_coord.write.mode("overwrite").parquet(friends_mart)
      

def main():
    user_geo_mart = sys.argv[1] # f"/user/{studentusername}/temp/geo/geo.csv"
    geo_events = sys.argv[2] #f"/user/{studentusername}/data/ref/geo"
    msg_geo_tmp = sys.argv[3]
    geo_ref_path = sys.argv[4]
    friends_mart = sys.argv[5]
    
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"prj-{studentusername}-fill_friends_mart") \
                        .getOrCreate()
 
    spark.sparkContext.setCheckpointDir(f"hdfs:///user/{studentusername}/temp")

    fill_friends_mart(user_geo_mart, geo_events, msg_geo_tmp, geo_ref_path, friends_mart, spark)

if __name__ == "__main__":
    main()

        