import pandas as pd
from sodapy import Socrata
import pyspark
import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

sc = SparkContext.getOrCreate()
spark=SparkSession(sc)


sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

client = Socrata("data.melbourne.vic.gov.au", None)
df = spark.createDataFrame(client.get("b2ak-trbp", limit=1000000))
location = spark.createDataFrame(client.get("h57g-5234", limit=20000))


daily = df.withColumn("hourly_counts", df["hourly_counts"].cast(IntegerType()))\
            .groupBy('sensor_id','day')\
            .sum('hourly_counts')\
            .orderBy('sum(hourly_counts)')\
            .groupBy('sensor_id')\
            .avg('sum(hourly_counts)')\
            .orderBy(F.col('avg(sum(hourly_counts))').desc())\
            .limit(10)\
            .withColumn('avg_daily_counts',F.col('avg(sum(hourly_counts))'))\
            .join(location,on='sensor_id',how="left")\
            .select('sensor_name','sensor_description','avg_daily_counts')\
            .orderBy(F.col('avg_daily_counts').desc())\
            .withColumn('avg_daily_counts',F.floor(F.col('avg_daily_counts')))
print(daily.show())
monthly = df.withColumn("hourly_counts", df["hourly_counts"].cast(IntegerType()))\
            .groupBy('sensor_id','month')\
            .sum('hourly_counts')\
            .orderBy('sum(hourly_counts)')\
            .groupBy('sensor_id')\
            .avg('sum(hourly_counts)')\
            .orderBy(F.col('avg(sum(hourly_counts))').desc())\
            .limit(10)\
            .withColumn('avg_monthly_counts',F.col('avg(sum(hourly_counts))'))\
            .join(location,on='sensor_id',how="left")\
            .select('sensor_name','sensor_description','avg_monthly_counts')\
            .orderBy(F.col('avg_monthly_counts').desc())\
            .withColumn('avg_monthly_counts',F.floor(F.col('avg_monthly_counts')))
print(monthly.show())
final_df = df.join(location, on='sensor_id')\
            .withColumn('date',F.regexp_extract(F.col('date_time'),'\d\d\d\d-\d\d-\d\d',0))

accessKeyId='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
secretAccessKey='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
aws_credentials={'key': accessKeyId, 'secret': secretAccessKey}

final_df=final_df.toPandas()
final_df=final_df.loc[:,~final_df.columns.duplicated()]\
            .to_parquet('s3://victoria-pedestrian-counter/DATA/',storage_options= aws_credentials,partition_cols='date')
sc.stop()
