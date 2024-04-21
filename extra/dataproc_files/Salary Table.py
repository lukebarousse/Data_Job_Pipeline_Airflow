from google.cloud import bigquery
from google.cloud import storage

import sparknlp
from sparknlp.annotator import Lemmatizer, Stemmer, Tokenizer, Normalizer, TextMatcher, DocumentNormalizer
from sparknlp.base import DocumentAssembler, Finisher

from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import LDA
from pyspark.sql.functions import split, regexp_replace, when, lower, array_distinct, monotonically_increasing_id, col, size
from pyspark.ml import Pipeline

import pandas as pd

# Start Spark Session
spark = SparkSession \
 .builder \
 .appName('BigQuery Storage & Spark DataFrames') \
 .getOrCreate()

# BigQuery Setup
dataset = 'gsearch_job_listings_clean'
table = 'job-listings-366015.gsearch_job_listings_clean.gsearch_jobs_fact'
BUCKET_NAME = 'dataproc-cluster-gsearch'
# need to read with SQL from table
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset", dataset)
# need to set temp bucket for writing
spark.conf.set("temporaryGcsBucket", BUCKET_NAME)

# Salary Cleanup
sql = """
  SELECT job_id, job_salary
  FROM `job-listings-366015.gsearch_job_listings_clean.gsearch_jobs_fact`
  WHERE job_salary IS NOT null
  """
salary = spark.read \
 .format("bigquery") \
 .load(sql)
# drop duplicates so when create final dimension table only one job_id to match on
salary = salary.dropDuplicates(['job_id'])

salary_clean = salary

# split column on ' '
split_col =  split(salary_clean.job_salary, ' ',)
salary_clean = salary_clean.withColumn('salary_pay', split_col.getItem(0))\
    .withColumn('salary_rate', split_col.getItem(2))\
    .drop('job_salary')

# remove , $, " "
salary_clean = salary_clean.withColumn('salary_pay', regexp_replace('salary_pay', ',', ''))
salary_clean = salary_clean.withColumn('salary_pay', regexp_replace('salary_pay', '$', ''))
salary_clean = salary_clean.withColumn('salary_pay', regexp_replace('salary_pay', ' ', ''))

# start creation of 'salary_avg' on columns with no "-"
# The character U+2013 "–" could be confused with the character U+002d "-", which is more common in source code.
salary_clean = salary_clean.withColumn(
    'salary_avg', 
    when(salary_clean.salary_pay.contains("–"), None)\
    .otherwise(salary_clean.salary_pay))

# create 'salary_min' & 'salary_max'column for cleaning
salary_clean = salary_clean.withColumn(
    'salary_', 
    when(salary_clean.salary_pay.contains("–"), salary_clean.salary_pay)\
    .otherwise(None))
split_col =  split(salary_clean.salary_, "–",)
salary_clean = salary_clean.withColumn('salary_min', split_col.getItem(0))\
    .withColumn('salary_max', split_col.getItem(1))\
    .drop('salary_')

# format to remove 'K', multiply by 1000
for column in ['salary_avg', 'salary_min', 'salary_max']:
    salary_clean = salary_clean.withColumn(
        column, 
        when(salary_clean[column].contains('K'), regexp_replace(column, 'K', '').cast('float')*1000)\
        .otherwise(salary_clean[column]))
    salary_clean = salary_clean.withColumn(column,salary_clean[column].cast('float'))

# update 'salary_avg' column to take avg of 'salary_min' and 'salary_max'
salary_clean = salary_clean.withColumn(
    'salary_avg', 
    when(salary_clean.salary_min.isNotNull(), 
    (salary_clean.salary_min + salary_clean.salary_max)/2)\
    .otherwise(salary_clean.salary_avg))
salary_clean = salary_clean.withColumn('salary_avg',salary_clean.salary_avg.cast('float'))

for rate in ['year', 'hour']:
    salary_clean = salary_clean.withColumn(
        'salary_'+rate, 
        when(salary_clean.salary_rate.contains(rate), salary_clean.salary_avg)\
        .otherwise(None))
   # salary_clean = salary_clean.withColumn('salary_'+rate,salary_clean['salary_'+rate].cast('float'))

print("Number of jobs with salary: ", salary_clean.count())
salary_clean.printSchema()

# Write to BigQuery
salary_clean.write.format('bigquery') \
  .option('table', 'job-listings-366015.gsearch_job_listings_clean.gsearch_salary') \
  .option('materializationExpirationTimeInMinutes', 60) \
  .mode('overwrite') \
  .save()