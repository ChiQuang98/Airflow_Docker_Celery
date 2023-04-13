import pyspark
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import numpy as np
import re
import pandas as pd
import datetime

spark = SparkSession.builder.appName('clean_data').getOrCreate()
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


print(df.count())
#loai bo cac cot khong can thiet
df1=df.drop(*['Thong tin chu phong','Dat coc','Trang thai','Title','Mo ta them','Luu y','Phi Wifi'])
df1.fillna(value="unknown")
#xu ly cot dia chi
def extract_district(text):
    Quan = "Quận"
    Huyen= "Huyện"
    splt = re.split(', ', text)
    x = splt[len(splt) - 2]
    if not ((Quan in x) or (Huyen in x)):
      x="No District"
    return x 
extract_district_udf = f.udf(extract_district, StringType())
df1=df1.withColumn('Quan', extract_district_udf(f.col('Dia chi')))
df1=df1.filter(~f.col("Quan").contains("No District"))
print("Distinct Count: ") 
df1.select(f.countDistinct("Quan")).show()
df1.select('Quan').distinct().show(41)


#xu ly don vi tien te
def currency_process(text):
  if text is not ("unknown" and None):
    text=text.replace(' đồng', '' )
    text=text.replace(',', '' )
    text=text.replace('Miễn phí', '0')
  else:
    text="-1"
  return text
def price_process(text):
  if float(text)>0 and float(text) <1000000:
    text= str(float(text)*1000)
  return text

currency_process_udf=f.udf(currency_process, StringType())
price_process_udf=f.udf(price_process, StringType())
df1=df1.withColumn('Gia phong', currency_process_udf(f.col('Gia phong')).cast(DoubleType())) \
  .withColumn('Gia phong', price_process_udf(f.col('Gia phong')).cast(DoubleType())) \
  .withColumn('Dien', currency_process_udf(f.col('Dien')).cast(DoubleType())) \
  .withColumn('Nuoc', currency_process_udf(f.col('Nuoc')).cast(DoubleType()))
df1=df1.withColumn('Dien tich', f.regexp_replace('Dien tich', ' mét vuông', '').cast(DoubleType()))
#xu ly truong ngay thang de luu vao postgres:
def datetime_process(text):
  text = datetime.datetime.strptime(text, '%d-%m-%Y').strftime('%Y-%m-%d')
  return text
datetime_process_udf = f.udf(datetime_process,StringType())
df1=df1.withColumn('Ngay dang', datetime_process_udf(f.col('Ngay dang')))

#xu ly tien dien, tien nuoc
def water_price_process(x):
   if x>0 and x<1000:
      x=x*1000
   if x>0 and x<=25000:
      x=x*6
   if x>200000:
      x=None
   return x
def electric_price_process(x):
  if x<1000:
      x=x*1000
  elif x>200000:
      x=None
  return x

water_price_process_udf = f.udf(water_price_process,DoubleType())
electric_price_process_udf= f.udf(electric_price_process, DoubleType())

df1=df1.withColumn('Nuoc', water_price_process_udf(f.col('Nuoc'))) \
    .withColumn('Dien', electric_price_process_udf(f.col('Dien')))  
water_mean = df1.filter(f.col('Nuoc') >25000).select(f.mean('Nuoc').alias('water_mean')).first().asDict()
elec_mean = df1.filter(f.col('Dien') <10000).select(f.mean('Dien').alias('elec_mean')).first().asDict()

print(water_mean)
print(elec_mean)

df1 = df1.fillna(value=water_mean.get('water_mean'), subset=['Nuoc'])
df1 = df1.fillna(value=elec_mean.get('elec_mean'), subset=['Dien'])
df1.select('Gia phong').distinct().show()
df1.select('Dien').distinct().show()
df1.select('Nuoc').distinct().show()

#xu ly truong suc chua
def has_male(text):
  if bool(re.search('Nam', text)) or bool(re.search('hoặc', text)):
    return 1
  else:
    return 0
def has_female(text):
  if bool(re.search('Nữ', text)) or bool(re.search('hoặc', text)):
    return 1
  else:
    return 0
has_male_udf=f.udf(has_male,IntegerType())
has_female_udf = f.udf(has_female, IntegerType())

df1=df1.withColumn('Nam', has_male_udf(f.col('Suc chua'))) \
  .withColumn('Nu', has_female_udf(f.col('Suc chua')))

def capacity_process(text):
  if text == None or not bool(re.search(r'\d', text)):
    return 0.0
  # patern=re.compile("[0-20]|-|, |,[0-20]")
  # text=patern.search(text)
  # splt = re.split('-|, |,', text)
  # text = splt[len(splt) - 1]
  text=re.sub(' Nam','',text)
  text=re.sub(' hoặc','',text)
  text=re.sub(' Nữ','',text)
  text=re.sub(' người','',text)
  text=re.sub('[...]','',text)
  #splt=re.split('-|, |,|[.]', text)
  splt=re.split('-|, |,|[.]', text)
  text=splt[len(splt) - 1]
  text=re.sub('\D','',text)
  print(text)
  if text == '':
    return 0.0
  return float(text)
capacity_process_udf = f.udf(capacity_process, DoubleType())

df1=df1.withColumn('Suc chua', capacity_process_udf(f.col('Suc chua')))
print(df1.filter(f.col('Suc chua')<20).filter(f.col('Gia phong')<30000000).filter(f.col('Dien tich')<200).count())
print(df1.filter(f.col('Suc chua')<20).count())
print(df1.filter(f.col('Gia phong')<30000000).count())
print(df1.filter(f.col('Dien tich')<200).count())
df1=df1.filter(f.col('Suc chua')<20).filter(f.col('Gia phong')<30000000).filter(f.col('Dien tich')<200)
df1.select('Suc chua').distinct().show(500)
df1.select('Nam').distinct().show(500)
df1.select('Nu').distinct().show(500)
df1=df1.drop(*['Dia chi'])
df1pd = df1.toPandas()
# df1.write.format("csv").mode('append').options(header='True', delimiter=',',escape ='\"') \
#  .save('../spark/resources/data/cleaned_house_price_data.csv')
df1pd.to_csv("../spark/resources/data/cleaned_house_price_data.csv", index=False, header=True)
spark.stop()


