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


# фукнция поиска расстояния между точками на шаре. Кажется, что можно воспользоваться теор. 
# Пифагора и не париться, так как разница не большая. 
def haversine_distance(df, lat1_col, lon1_col, lat2_col, lon2_col):
    # Радиус Земли в километрах
    R = 6371.0
    
    # Преобразуем координаты из градусов в радианы
    df = df.withColumn("lat1_rad", F.radians(F.col(lat1_col)))
    df = df.withColumn("lon1_rad", F.radians(F.col(lon1_col)))
    df = df.withColumn("lat2_rad", F.radians(F.col(lat2_col)))
    df = df.withColumn("lon2_rad", F.radians(F.col(lon2_col)))
    
    # Вычисление разностей координат
    df = df.withColumn("delta_lat", F.col("lat2_rad") - F.col("lat1_rad"))
    df = df.withColumn("delta_lon", F.col("lon2_rad") - F.col("lon1_rad"))
    
    # Применение формулы гаверсинуса
    df = df.withColumn("a", 
                       F.sin(F.col("delta_lat") / 2) ** 2 +
                       F.cos(F.col("lat1_rad")) * F.cos(F.col("lat2_rad")) *
                       F.sin(F.col("delta_lon") / 2) ** 2)
    
    df = df.withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a"))))
    
    # Вычисляем окончательное расстояние
    df = df.withColumn("distance_km", F.col("c") * R)
    
    # Удаляем временные колонки
    df = df.drop("lat1_rad", "lon1_rad", "lat2_rad", "lon2_rad", "delta_lat", "delta_lon", "a", "c")
    
    return df

# получаем места отправки сообщений и добавляем их к сообщениям
def fill_msg_with_city(geo_ref_path, geo_events,msg_geo_tmp, spark):
    
    geo = spark.read.parquet(geo_ref_path)
#     log_df(geo)
    
    #and event.message_channel_to is not null
    df = spark.read.parquet(geo_events)\
    .where("event_type='message'")\
     .withColumn("datetime", F.coalesce(
        F.to_timestamp(F.col("event.message_ts"), "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
        F.to_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
        F.col("date")
    ))\
    .selectExpr(
            "event.message_id as message_id",
            "event.message_from as user_id",            
            "date",
            "datetime",
            "lat event_lat",
            "lon event_lon"
        )\
   
                
    df_geo = df.crossJoin(geo)\
    .withColumnRenamed("lat", "city_lat")\
    .withColumnRenamed("lon", "city_lon")\
    

#     log_df(df_geo)

    df_with_distance = haversine_distance(df_geo, "event_lat", "event_lon", "city_lat", "city_lon")
    msg_city = df_with_distance.\
    withColumn("rank", F.row_number().over(Window.partitionBy("message_id", "user_id", "datetime").\
                                                            orderBy(F.asc("distance_km")))).\
    where("rank = 1").\
    selectExpr(
            "message_id",
            "user_id",
            "datetime",
            "city",
            "event_lat lat", 
            "event_lon lon", # пригодится в последнем задании для поиска друзей
            "date"
        )
    
#     log_msg(msg_city)

    # Дико тормозит расчет. Сохраняем и работаем с ним, как с промежуточными данными
    # сохраняем данные в "fill_msg_with_city"
    
    msg_city.write.option("header",True) \
    .partitionBy( "date")  \
    .mode("overwrite") \
    .parquet(msg_geo_tmp)
    
          

def main():
    geo_ref_path = sys.argv[1] # f"/user/{studentusername}/temp/geo/geo.csv"
    geo_events = sys.argv[2] #f"/user/{studentusername}/data/ref/geo"
    msg_geo_tmp = sys.argv[3]
    
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"prj-{studentusername}-fill_msg_with_city") \
                        .getOrCreate()
 
    spark.sparkContext.setCheckpointDir(f"hdfs:///user/{studentusername}/temp")

    fill_msg_with_city(geo_ref_path, geo_events,msg_geo_tmp, spark)

if __name__ == "__main__":
    main()

        