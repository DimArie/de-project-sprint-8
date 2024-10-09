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
from pyspark import StorageLevel

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName(f"project-07-03") \
                    .config("spark.executor.memory", "4g") \
                    .config("spark.executor.cores", "4")\
                    .config("spark.driver.cores", "4")\
                    .getOrCreate()

studentusername = 'dimarieisr'


spark.sparkContext.setCheckpointDir(f"hdfs:///user/{studentusername}/temp")


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

debug = 1

def log_msg(msg):
    if debug == 1:
        print(msg) # можно логгер поставить. Пока лень

def log_df(df): # понимаем что эта хрень вызывает расчет данных
    
    if debug == 1:
#         print(f"df.count() = {df.count()}")
        df.printSchema()
        df.show(50, False)    
        
        
# Функция для проверки, является ли это действительной временной зоной
def validate_timezone(city):
    timezone = f"Australia/{city}"
    if timezone in pytz.all_timezones:
        return timezone
    else:
        return "Australia/Sydney"  # Вернуть стандартную зону, если город не в списке

# подгружаем, корректируем данные по городам. Дополняем необходимой инфой
def fill_geo_ods():
    geo = spark.read.csv(path = stg_geo_ref_path, sep=";", inferSchema=True, header=True)

    # Применение функции для добавления столбца с корректной временной зоной
    validate_timezone_udf = F.udf(validate_timezone)
    
    geo = geo.withColumn("lat", F.expr("cast(replace(lat, ',', '.') as double)")) \
         .withColumn("lon", F.expr("cast(replace(lng, ',', '.') as double)")) \
         .withColumn("timezone", validate_timezone_udf(F.col("city")))\
         .drop("lng")

    log_df(geo)
        
    geo.write.mode("overwrite").parquet(geo_ref_path)
    

# перекладываем данные из источника в ODS
def fill_geo_events_ods():
    df = spark.read.load(masterdata_geo_events) 
    
    df.write.option("header",True) \
    .partitionBy( "date", "event_type") \
    .mode("overwrite") \
    .parquet(geo_events) 

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
def fill_msg_with_city():
    
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
   
                
    # То ли я где-то просмотрел занятия, но большую часть времени занимаюсь изучением данных
    # может в прочем эта дата не нужна

    ##.sample(withReplacement=False, fraction=0.01)

#     log_df(df)

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
        ).checkpoint()
    
#     log_msg(msg_city)

    # Дико тормозит расчет. Сохраняем и работаем с ним, как с промежуточными данными
    # сохраняем данные в "fill_msg_with_city"
    
    msg_city.write.option("header",True) \
    .partitionBy( "date")  \
    .mode("overwrite") \
    .parquet(msg_geo_tmp)
    
def fill_days_in_town():
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
    .cache()
#     log_df(df_unique_days)
#     .orderBy(F.asc("user_id"),F.asc("city"), F.asc("first_date"))\

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
    .cache()
    
        
#     log_df(df_days_in_city)
    
    # получаем список актуальных городов и последнюю активность
    df_act_city = df_unique_days\
    .withColumn("act_city", F.last("city").over(Window.partitionBy("user_id").orderBy("last_date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))\
    .groupBy("user_id", "act_city")\
    .agg(F.min("last_action_datetime").alias("last_action_time"))\
    .cache()
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
    .cache()
    
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
    .cache()
    
#     log_df(df_out)
    # бьем по act_city.
    # Как будто кусок задания: "Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя."
    # подразумевает что городом пользователя будем считать его последний город пребывания
    df_out.write.option("header",True) \
    .partitionBy( "act_city") \
    .mode("overwrite") \
    .parquet(user_geo_mart) 
    

# def input_event_paths(date, depth, path):
#     dt = datetime.strptime(date, '%Y-%m-%d')
#     return [f"{path}/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]
    
    
def fill_zone_mart():
    
    # По условию задачи 3
    # "Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя."
    # Я делаю вывод что отсюда можно взять пользователей региона по act_city
    df_geo_mart = spark.read.parquet(user_geo_mart)\
    .selectExpr("user_id", "act_city as city")\
    .sample(withReplacement=False, fraction=0.1, seed=20)\
    .persist(StorageLevel.DISK_ONLY)
    
    log_df(df_geo_mart)
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
       
    log_df(df)
    
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
    
    print("df_geo_msg_first")
    log_df(df_geo_msg_first)
    
    df_pivot_week = df.groupBy("city", "week", "month") \
    .pivot("event_type") \
    .agg(F.count("*").alias("event_count")) \
    .withColumnRenamed("subscription", "week_subscription")\
    .withColumnRenamed("reaction", "week_reaction")\
    .withColumnRenamed("message", "week_message")\
    .persist(StorageLevel.DISK_ONLY)
    
    print("df_pivot_week")
    log_df(df_pivot_week) 
    
    df_pivot_month = df.groupBy("city", "month") \
    .pivot("event_type") \
    .agg(F.count("*").alias("event_count")) \
    .withColumnRenamed("subscription", "month_subscription")\
    .withColumnRenamed("reaction", "month_reaction")\
    .withColumnRenamed("message", "month_message")\
    .persist(StorageLevel.DISK_ONLY)
    
    print("df_pivot_month")
    log_df(df_pivot_month) 
    
    # объединяем данные. + добавляем город 
    res = df_pivot_week\
    .join(df_geo_msg_first.select("week", "city", "user"), ["week", "city"], how="left")\
    .join(df_pivot_month, ["month", "city"], how="left")\
    .join(df_geo_msg_first.groupBy("month", "city").agg(F.sum("user").alias("month_user")), ["month", "city"], how="left")\
    .withColumnRenamed("user", "week_user")
    
    
    # ToDo: сохраняем выборку куда-то в analytics
#     log_df(res) 
    res.write.mode("overwrite").parquet(zones_mart)

###
### считаем последнюю витрину
###
def fill_friends_mart():
   

    # собираем выборку для всех пользователей с городами.
    # чтобы не уложить сервер, делаем допущение, что люди, 
    # живущие на расстоянии одного км друг от друга живут в одном городе
    df_geo_mart = spark.read.parquet(user_geo_mart)\
    .select("user_id", "home_city")\
    .sample(withReplacement=False, fraction=0.1, seed=20)\
    .checkpoint()
      
    # получаем каналы (подписки)
    df = spark.read.parquet(geo_events)\
    .where("event_type='subscription'")\
    .selectExpr(
            "event.subscription_channel as channel",
            "event.user as user_id"
        )\
    .sample(withReplacement=False, fraction=0.1, seed=20)\
    .checkpoint()
   
#     log_df(df)
    
    # цепляем к данным по пользователям каналы.
    df = df_geo_mart.join(df, "user_id", how="inner")

#     log_df(df)
#     print(df.count())
    
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
    .checkpoint()

    
#     print(df_joined.count())
#     log_df(df_joined)
    
    
    # берем все личные сообщения
    df = spark.read.parquet(geo_events)\
    .where("event_type='message' AND event.message_to > 0")\
    .selectExpr(
            "event.message_from as user_id",  
            "event.message_to as contact_id",      
        )\
    .distinct()\
    .sample(withReplacement=False, fraction=0.001, seed=20).checkpoint()
    
#     log_df(df)

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
    .checkpoint()
    
     
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
    .checkpoint()
#     получаем все события с координатами

    log_df(df_coord)
    
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
    
    # сохраняем результаты
    df_coord.write.mode("overwrite").parquet(zones_mart)
    

#     log_df(df_coord)
        
# fill_geo_ods() 

# geo = spark.read.parquet(geo_ref_path)
# log_df(geo)
    
        
# перекачиваем данные из Мастерданных
# fill_geo_events_ods()

# опять обсчитываем расстояния и пытаемся найти кто откуда писал.
# fill_msg_with_city()


# Заполняем первую витрину

# fill_days_in_town()


# Заполняем вторую витрину
# city = 'Sydney'
    
fill_zone_mart()


# geo = spark.read.parquet(geo_ref_path)
# log_df(geo)
# fill_friends_mart()


# hdfs dfs -rm -f /user/dimarieisr/temp/geo/geo.csv

# hdfs dfs -put /lessons/geo.csv  /user/dimarieisr/temp/geo


