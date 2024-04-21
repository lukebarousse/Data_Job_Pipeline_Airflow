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

"""
Start Spark Session
"""
spark = SparkSession \
 .builder \
 .appName('BigQuery Storage & Spark DataFrames') \
 .getOrCreate()


"""
#BigQuery Setup
"""
dataset = 'gsearch_job_listings_clean'
table = 'job-listings-366015.gsearch_job_listings_clean.gsearch_jobs_fact'
BUCKET_NAME = 'dataproc-cluster-gsearch'
# need to read with SQL from table
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset", dataset)
# need to set temp bucket for writing
spark.conf.set("temporaryGcsBucket", BUCKET_NAME)

"""
Keywords Setup
"""
# Keywords from hand picking from data and Stack Overflow survey https://survey.stackoverflow.co/2022/#technology-most-popular-technologies            
keywords_programming = [
    'sql', 'python', 'r', 'c', 'c#', 'javascript', 'java', 'scala', 'sas', 'matlab', 
    'c++', 'perl', 'go', 'typescript', 'bash', 'html', 'css', 'php', 'powershell', 'rust', 
    'kotlin', 'ruby',  'dart', 'assembly', 'swift', 'vba', 'lua', 'groovy', 'delphi', 'objective-c', 
    'haskell', 'elixir', 'julia', 'clojure', 'solidity', 'lisp', 'f#', 'fortran', 'erlang', 'apl', 
    'cobol', 'ocaml', 'crystal',  'golang', 'nosql', 'mongodb', 't-sql', 'no-sql', 
    'pascal', 'mongo', 'sass', 'vb.net', 'shell', 'visual basic',
]
# 'js',  'c/c++', 'pl/sql', 'javascript/typescript', 'visualbasic', 'objective c', 

keywords_databases = [
    'mysql', 'sql server', 'postgresql', 'sqlite', 'mongodb', 'redis', 'mariadb',
    'elasticsearch', 'firebase', 'dynamodb', 'firestore', 'cassandra', 'neo4j', 'db2', 
    'couchbase', 'couchdb', 
]
# 'mssql', 'sqlserver', 'postgres', 

keywords_cloud = [
    'aws', 'azure', 'gcp', 'firebase', 'heroku', 'digitalocean', 'vmware', 'managedhosting',
    'linode', 'ovh', 'oracle', 'openstack', 'watson', 'colocation', 
    'snowflake', 'redshift', 'bigquery', 'aurora', 'databricks', 'ibm cloud',
]
# 'googlecloud', 'google cloud', 'oraclecloud',  'oracle cloud'  'amazonweb', 'amazon web', 'ibmcloud', 

keywords_libraries = [
    'scikit-learn', 'jupyter', 'theano', 'openCV', 'pyspark', 'nltk', 'mlpack', 'chainer', 'fann', 'shogun', 
    'dlib', 'mxnet', 'keras', '.net', 'numpy', 'pandas', 'matplotlib', 'spring', 'tensorflow', 'flutter', 
    'react', 'kafka', 'electron', 'pytorch', 'qt', 'ionic', 'xamarin', 'spark', 'cordova', 'hadoop', 'gtx',
    'capacitor', 'tidyverse', 'unoplatform', 'dplyr', 'tidyr', 'ggplot2', 'plotly', 'rshiny', 'mlr',
    'airflow', 'seaborn', 'gdpr', 'graphql', 'selenium', 'hugging face', 'uno platform'

]
# 'huggingface', 

keywords_webframeworks = [
    'node.js', 'vue', 'vue.js', 'ember.js', 'node', 'jquery', 'asp.net', 'react.js', 'express',
    'angular', 'asp.netcore', 'django', 'flask', 'next.js', 'laravel', 'angular.js', 'fastapi', 'ruby', 
    'svelte', 'blazor', 'nuxt.js', 'symfony', 'gatsby', 'drupal', 'phoenix', 'fastify', 'deno', 
    'asp.net core', 'ruby on rails', 'play framework',
]
# 'jse/jee', 'rubyonrails', 'playframework',

keywords_os = [
    'unix', 'linux', 'windows', 'macos', 'wsl', 'ubuntu', 'centos', 'debian', 'redhat', 
    'suse', 'fedora', 'kali', 'arch',
]
# 'unix/linux', 'linux/unix', 

keywords_analyst_tools = [
    'excel', 'tableau', 'word', 'powerpoint', 'looker', 'power bi', 'outlook', 'sas', 'sharepoint', 'visio',  
    'spreadsheet', 'alteryx', 'ssis', 'spss', 'ssrs', 'microstrategy',  'cognos', 'dax',  
    'esquisse', 'sap', 'splunk', 'qlik', 'nuix', 'datarobot', 'ms access', 'sheets',
]
# 'powerbi', 'powerpoints', 'spreadsheets', 

keywords_other = [
    'npm', 'docker', 'yarn', 'homebrew', 'kubernetes', 'terraform', 'unity', 'ansible', 'unreal', 'puppet',
    'chef', 'pulumi', 'flow', 'git', 'svn', 'gitlab', 'github', 'jenkins', 'bitbucket', 'terminal', 'atlassian',
    'codecommit',
]

keywords_async = [
    'jira', 'confluence', 'trello', 'notion', 'asana', 'clickup', 'planner', 'monday.com', 'airtable', 'smartsheet',
    'wrike', 'workfront', 'dingtalk', 'swit', 'workzone', 'projectplace', 'cerri', 'wimi', 'leankor', 'microsoft lists'
]
# 'microsoftlists', 

keywords_sync = [
    'slack', 'microsoft teams', 'twilio', 'zoom', 'webex', 'mattermost', 'rocketchat', 'ringcentral',
    'symphony', 'wire', 'wickr', 'unify', 'coolfire', 'google chat', 
]

# keywords_skills = [
# 'coding', 'server', 'database', 'cloud', 'warehousing', 'scrum', 'devops', 'programming', 'saas', 'ci/cd', 'cicd', 
# 'ml', 'data_lake', 'frontend',' front-end', 'back-end', 'backend', 'json', 'xml', 'ios', 'kanban', 'nlp',
# 'iot', 'codebase', 'agile/scrum', 'agile', 'ai/ml', 'ai', 'paas', 'machine_learning', 'macros', 'iaas',
# 'fullstack', 'dataops', 'scrum/agile', 'ssas', 'mlops', 'debug', 'etl', 'a/b', 'slack', 'erp', 'oop', 
# 'object-oriented', 'etl/elt', 'elt', 'dashboarding', 'big-data', 'twilio', 'ui/ux', 'ux/ui', 'vlookup', 
# 'crossover',  'data_lake', 'data_lakes', 'bi', 
# ]

# put all keywords in a dict
keywords_dict = {
 'keywords_programming': keywords_programming, 'keywords_databases': keywords_databases, 'keywords_cloud': keywords_cloud, 
 'keywords_libraries': keywords_libraries, 'keywords_webframeworks': keywords_webframeworks, 'keywords_os': keywords_os, 
 'keywords_analyst_tools': keywords_analyst_tools, 'keywords_other': keywords_other, 'keywords_async': keywords_async,
 'keywords_sync': keywords_sync
}

# create a list of all keywords
keywords_all = [item for sublist in keywords_dict.values() for item in sublist] 
# add keywords_all to dict
keywords_dict['keywords_all'] = keywords_all

"""
Keywords Save
"""
# write keywords to google storage bucket
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)

# save all keywords variations to different files
for key, value in keywords_dict.items():
    keywords_df = pd.DataFrame(value, index=None)
    bucket.blob(f'notebooks/jupyter/keywords/{key}.txt').upload_from_string(keywords_df.to_csv(index=False, header=False) , content_type='text/csv')

"""
Create Keywords Dataframe
"""
# using SQL to read data to ensure JSON values not selected (throws error)
# delete where statement to process all jobs over again
sql = """
  SELECT job_id, job_description
  FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact
  WHERE job_id NOT IN
    (SELECT job_id FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills)
  """
skills = spark.read \
 .format("bigquery") \
 .load(sql)

# drop duplicates so when create final dimension table only one job_id to match on
skills = skills.dropDuplicates(['job_id'])

# lowercase the description here for pre-cleanup before model (couldn't get to work with SparkNLP lib)
skills = skills.withColumn('job_description', lower(skills.job_description))

# make final dataframe to append to
skills_final = skills
skills_final = skills_final.withColumn("id", monotonically_increasing_id())
skills_final = skills_final.alias('skills_final')

for keyword_name, keyword_list in keywords_dict.items():

    # Makes document tokenizable
    document_assembler = DocumentAssembler() \
        .setInputCol("job_description") \
        .setOutputCol("document") 

    # Capture multi-words as one word
    # can't have space in between or won't pick up multiple word
    multi_words = [
        'power_bi', 'sql_server', 'google_cloud', 'visual_basic', 'oracle_cloud',
        'ibm_cloud', 'hugging_face', 'uno_platform', 'microsoft_lists', 'ms_access',
        'microsoft_teams', 'google_chat', 'asp.net_core', 'ruby_on_rails', 'play_framework', 
    ]
    # 'objective_c', 'amazon_web'
    
    # Tokenizes document with multi_word exceptions
    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token") \
        .setCaseSensitiveExceptions(False) \
        .setExceptions(multi_words)

    # Get all the keywords we defined above
    keywords_file = f'gs://dataproc-cluster-gsearch/notebooks/jupyter/keywords/{keyword_name}.txt'
    keywords_all = TextMatcher() \
        .setInputCols(["document", "token"])\
        .setOutputCol("matcher")\
        .setEntities(keywords_file)\
        .setCaseSensitive(False)\
        .setEntityValue(keyword_name)

    # Make output machine readable
    finisher = Finisher() \
        .setInputCols(["matcher"]) \
        .setOutputCols([keyword_name]) \
        .setValueSplitSymbol(" ")

    pipeline = Pipeline(
        stages = [
            document_assembler,
            tokenizer,
            keywords_all,
            finisher,
        ]
    )

    # Fit the data to the model.
    model = pipeline.fit(skills).transform(skills)

    # Remove duplicate tokens, was unable to figure out how to do this in the model
    model = model.withColumn(keyword_name, array_distinct(keyword_name))
    
    from pyspark.sql.types import StringType, ArrayType
    model = model.withColumn(keyword_name, when(size(col(keyword_name))==0 , None).otherwise(col(keyword_name)))
    
    # Drop unnecessary columns prior to join to final df
    model = model.drop('job_id', 'job_description')
    
    # Assign index number for joining
    model = model.withColumn("id", monotonically_increasing_id())
    
    # Join model df with the final df
    skills_final = skills_final.join(model, ['id'])

# No longer need id column
skills_final = skills_final.drop('id')
print("Number of jobs with skills: ", skills_final.count())

"""
Write to BigQuery
"""
skills_final.write.format('bigquery') \
  .option('table', 'job-listings-366015.gsearch_job_listings_clean.gsearch_skills') \
  .option('materializationExpirationTimeInMinutes', 180) \
  .mode('append') \
  .save()   
# 'overwrite' or 'append'