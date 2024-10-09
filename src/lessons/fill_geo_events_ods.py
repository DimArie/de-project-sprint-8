
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


# перекладываем данные из источника в ODS
def fill_geo_events_ods(masterdata_geo_events, geo_events, spark):
    df = spark.read.load(masterdata_geo_events) 
    
    df.write.option("header",True) \
    .partitionBy( "date", "event_type") \
    .mode("overwrite") \
    .parquet(geo_events) 



def main():
    masterdata_geo_events = sys.argv[1] # f"/user/{studentusername}/temp/geo/geo.csv"
    geo_events = sys.argv[2] #f"/user/{studentusername}/data/ref/geo"
      
    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"prj-{studentusername}-fill_geo_events_ods") \
                        .getOrCreate()
 
    
    fill_geo_events_ods(masterdata_geo_events, geo_events, spark)

if __name__ == "__main__":
    main()

        