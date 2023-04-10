import pyspark
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import numpy as np
import re
import pandas as pd
import datetime

spark = SparkSession.builder.appName('save_data').getOrCreate()
sqlContext = SQLContext(spark)
schema = StructType([ \
    StructField("Loai phong", StringType(), True), \
    StructField("Title", StringType(), True), \
    StructField("Mo ta them", StringType(), True), \
    StructField("Thong tin chu phong", StringType(), True), \
    StructField("Ngay dang", StringType(), True), \
    StructField("Luu y", StringType(), True), \
    StructField("Gia phong", StringType(), True), \
    StructField("Dien tich", StringType(), True), \
    StructField("Dat coc", StringType(), True), \
    StructField("Suc chua", StringType(), True), \
    StructField("Trang thai", StringType(), True), \
    StructField("Dia chi", StringType(), True), \
    StructField("Dien", StringType(), True), \
    StructField("Nuoc", StringType(), True), \
    StructField("Phi Wifi", StringType(), True), \
    StructField("May lanh", StringType(), True), \
    StructField("WC rieng", StringType(), True), \
    StructField("Cho de xe", StringType(), True), \
    StructField("Wifi", StringType(), True), \
    StructField("Tu do", StringType(), True), \
    StructField("Khong chung chu", StringType(), True), \
    StructField("Tu lanh", StringType(), True), \
    StructField("May giat", StringType(), True), \
    StructField("Bao ve", StringType(), True), \
    StructField("Giuong ngu", StringType(), True), \
    StructField("Nau an", StringType(), True), \
    StructField("Tivi", StringType(), True), \
    StructField("Thu cung", StringType(), True), \
    StructField("Tu quan ao", StringType(), True), \
    StructField("Cua so", StringType(), True), \
    StructField("May nuoc nong", StringType(), True), \
    StructField("Gac lung", StringType(), True) \
  ])
df = spark.read.option("delimiter", ",") \
      .option("escape", "\"") \
      .option("multiLine","true") \
      .format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("../spark/resources/data/house_price_data.csv")

df.write.save('hdfs://0.0.0.0:9000/data_folderer/house_price.csv', format='csv', mode='overwrite')
spark.stop()