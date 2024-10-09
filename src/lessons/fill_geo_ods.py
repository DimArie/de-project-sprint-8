
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


studentusername = 'dimarieisr'


# Функция для проверки, является ли это действительной временной зоной
def validate_timezone(city):
    timezone = f"Australia/{city}"
    if timezone in pytz.all_timezones:
        return timezone
    else:
        return "Australia/Sydney"  # Вернуть стандартную зону, если город не в списке

# подгружаем, корректируем данные по городам. Дополняем необходимой инфой
def fill_geo_ods(stg_geo_ref_path, geo_ref_path, spark):
    geo = spark.read.csv(path = stg_geo_ref_path, sep = ";", inferSchema = True, header = True)

    # Применение функции для добавления столбца с корректной временной зоной
    validate_timezone_udf = F.udf(validate_timezone)
    
    geo = geo.withColumn("lat", F.expr("cast(replace(lat, ',', '.') as double)")) \
         .withColumn("lon", F.expr("cast(replace(lng, ',', '.') as double)")) \
         .withColumn("timezone", validate_timezone_udf(F.col("city")))\
         .drop("lng")

#     log_df(geo)
        
    geo.write.mode("overwrite").parquet(geo_ref_path)

def main():
    stg_geo_ref_path = sys.argv[1] # f"/user/{studentusername}/temp/geo/geo.csv"
    geo_ref_path = sys.argv[2] #f"/user/{studentusername}/data/ref/geo"
      
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"prj-{studentusername}-fill_geo_ods") \
                        .getOrCreate()
 
    
    fill_geo_ods(stg_geo_ref_path, geo_ref_path, spark)

if __name__ == "__main__":
    main()

        