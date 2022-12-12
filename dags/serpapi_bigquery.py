"""
An operations workflow that you can deploy into Airflow to collect data science
job postings from around the world.
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

import time
from datetime import datetime, timedelta
import pandas as pd
from numpy.random import choice
from serpapi import GoogleSearch
from config import config  # contains secret keys in config.py
from google.cloud import bigquery
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from modules.country import view_percent

# 'False' dag is ready for operation; 'True' dag runs with no SerpApi or BigQuery requests
TESTING_DAG = False
# Minutes to sleep on an error
ERROR_SLEEP_MIN = 30 
# Max number of searches to perform daily
MAX_SEARCHES = 1500
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "airflow"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = ['luke@lukebarousse.com']
START_DATE = airflow.utils.dates.days_ago(1)


default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': START_DATE, 
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # removing retries to not call insert duplicates into BigQuery
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2022, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'serpapi_bigquery', 
    description='Call SerpApi and inserts results into Bigquery',
    default_args=default_args, 
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['data-pipeline-dag'],
    max_active_tasks = 3
)

with dag:

    search_terms = ['Data Analyst', 'Data Scientist', 'Data Engineer']
    search_locations_us = ["New York, United States", "California, United States", 
    "Texas, United States", "Illinois, United States", "Florida, United States"]
    # data table from 'modules/country.py' of countries and relative weighted percentages to use
    country_percent = view_percent()

    start = DummyOperator(
        task_id='start',
        dag=dag)

    def _serpapi_bigquery(search_term, search_location, search_time):
        
        if not TESTING_DAG:

            for num in range(45): # SerpApi docs say max returns is ~45 pages

                print(f"START API CALL: {search_term} in {search_location} on search {num}")

                start = num * 10
                error = False
                params = {
                    "api_key": config.serpapi_key,
                    "device": "desktop",
                    "engine": "google_jobs",
                    "google_domain": "google.com",
                    "q": search_term,
                    "hl": "en",
                    "gl": "us",
                    "location": search_location,
                    "chips": search_time,
                    "start": start,
                }

                # try except statement to call SerpAPI and then handle results (inner try/except statement) or handle TimeOut errors
                try:
                    search = GoogleSearch(params)
                    results = search.get_dict()

                    # try except statement needed to handle whether any results are returned
                    try:
                        if results['error'] == "Google hasn't returned any results for this query.":
                            print(f"END SerpApi CALLS: {search_term} in {search_location} on search {num}")
                            error = True
                            break
                    except KeyError:
                        print(f"SUCCESS SerpApi CALL: {search_term} in {search_location} on search {num}")
                    else:
                        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                        print(f"SerpApi Error on call!!!: No response on {search_term} in {search_location} on search {num}")
                        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                        break
                except Exception as e: # catching as 'TimeoutError' didn't work so resorted to catching all...
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print(f"SerpApi ERROR (Timeout)!!!: {search_term} in {search_location} had an error (most likely TimeOut)!!!")
                    print("Following error returned:")
                    print(e)
                    print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    time.sleep(ERROR_SLEEP_MIN * 60)
                    break

                # create dataframe of 10 (or less) pulled results
                jobs = results['jobs_results']
                jobs = pd.DataFrame(jobs)
                jobs = pd.concat([pd.DataFrame(jobs), 
                                pd.json_normalize(jobs['detected_extensions'])], 
                                axis=1).drop('detected_extensions', 1)
                jobs['date_time'] = datetime.utcnow()

                if start == 0:
                    jobs_all = jobs
                else:
                    jobs_all = pd.concat([jobs_all, jobs])

                jobs_all['search_term'] = search_term
                jobs_all['search_location'] = search_location

                # don't call api again (and waste a credit) if less than 10 results (i.e., end of search)
                if len(jobs) != 10:
                    print(f"END API CALLS: Only {len(jobs)} jobs (<10) for {search_term} in {search_location} on search {num}")
                    break

            # if no results returned on first try then will get error if try to insert 0 rows into BigQuery
            if num == 0 and error:
                print(f"NO DATA LOADED: {num} rows of {search_term} in {search_location} not loaded into BigQuery")
            else:
                try:
                    table_id = config.table_id
                    client = bigquery.Client()
                    table = client.get_table(table_id)
                    errors = client.insert_rows_from_dataframe(table, jobs_all)
                    if errors == [[]]:
                        print(f"DATA LOADED: {len(jobs_all)} rows of {search_term} in {search_location} loaded into BigQuery")
                    else:
                        print(f"ERROR!!!: {len(jobs_all)} rows of {search_term} in {search_location} NOT loaded into BigQuery!!!")
                        print("Following error returned from the googles:")
                        print(errors)
                except UnboundLocalError as ule:
                    # TODO: Need to build something to catch this error sooner
                    # GoogleSearch(params) code returns blank results and then get error for  "'jobs_all' referenced before assignment" in 'errors = client.insert_rows_from_dataframe(table, jobs_all)' (i.e., SerpApi issue)
                    print(f"SerpApi ERROR!!!: Search {num} of {search_term} in {search_location} yielded no results from SerpApi and FAILED load into BigQuery!!!")
                    print("Following error returned:")
                    print(ule)
                    # no sleep requirement as usually an issue with search term provided
                except TimeoutError as te:
                    # client.get_table(table_id) code returns TimeOut Exception with no results... so also adding sleep (i.e., BigQuery issue)
                    print(f"BigQuery ERROR!!!: {search_term} in {search_location} had TimeOutError and FAILED to load into BigQuery!!!")
                    print("Following error returned:")
                    print(te)
                    print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
                    time.sleep(ERROR_SLEEP_MIN * 60)
                except Exception as e:
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print(f"BigQuery ERROR!!!: {search_term} in {search_location} had an error that needs to be investigated!!!")
                    print("Following error returned:")
                    print(e)
                    print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    time.sleep(ERROR_SLEEP_MIN * 60)

            num_searches = num + 1
        
        else: # if testing

            print(f"END FAKE SEARCH: {search_term} in {search_location}")
            num_searches = 2 # low enough not to max out 1000 searches

        return num_searches

    def _us_jobs(search_terms, search_locations_us, **context):
        search_time = "date_posted:today"
        total_searches = 0

        for search_term in search_terms:
            for search_location in search_locations_us:
                print(f"START SEARCH: {total_searches} searches done, starting search...")
                num_searches = _serpapi_bigquery(search_term, search_location, search_time)   
                total_searches += num_searches

        context['task_instance'].xcom_push(key='total_searches', value=total_searches)

        return

    us_jobs = PythonOperator(
        task_id='us_jobs',
        provide_context=True,
        op_kwargs={'search_terms': search_terms, 'search_locations_us': search_locations_us},
        python_callable=_us_jobs
    )

    def _non_us_jobs(search_terms, country_percent, **context):
        search_time = "date_posted:today"
        total_searches = context['task_instance'].xcom_pull(task_ids='us_jobs', key='total_searches')
        search_countries = list(country_percent.country)
        search_probabilities = list(country_percent.percent)

        # create list of countries listed based on weighted probability to get random countries
        search_locations = list(choice(search_countries, size=len(search_countries), replace=False, p=search_probabilities))

        for search_location in search_locations:
            if total_searches < MAX_SEARCHES:
                print("####################################")
                print(f"SEARCHING COUNTRY: {search_location} [{search_locations.index(search_location)+1} of {len(search_locations)}]")
                print("####################################")
                for search_term in search_terms:
                    print("####################################")
                    print(f"SEARCHING TERM: {search_term}")
                    print(f"Starting search number {total_searches}...")
                    num_searches = _serpapi_bigquery(search_term, search_location, search_time)   
                    total_searches += num_searches
            else:
                print(f"STICK A FORK IN ME, I'M DONE!!!!: {total_searches} searches complete")
                return

    non_us_jobs = PythonOperator(
        task_id='non_us_jobs',
        provide_context=True,
        op_kwargs={'search_terms': search_terms, 'country_percent': country_percent},
        python_callable=_non_us_jobs
    )

    finish = DummyOperator(
        task_id='finish',
        dag=dag)

    start >> us_jobs >> non_us_jobs >> finish
