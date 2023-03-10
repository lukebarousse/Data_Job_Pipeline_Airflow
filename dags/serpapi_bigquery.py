"""
An operations workflow to collect data science job postings from SerpApi and
insert into BigQuery.
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

import time
import json
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

# 'False' DAG is ready for operation; i.e., 'True' DAG runs using no SerpApi credits or BigQuery requests
TESTING_DAG = False
# Minutes to sleep on an error
ERROR_SLEEP_MIN = 5 
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
    'email_on_failure': True,
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

    def _bigquery_json(results, search_term, search_location, result_offset, error):
        """
        Submit JSON return from SerpAPI to BigQuery {gsearch_jobs_all_json} as a backup to hold the original data
        
        Args:
            results : json
                JSON return from SerpAPI
            search_term : str
                Search term
            search_location : str
                Search location
            result_offset : int
                Parameter to offset the results returned from SerpApi; used for pagination
            error : bool
                Flag to indicate if results where returned from SerpApi or not
        
        Returns:
            None
        """               
        try:
            # extract metadata from results
            try:
                search_id = results['search_metadata']['id']
                search_time = results['search_metadata']['created_at']
                search_time = datetime.strptime(search_time, "%Y-%m-%d %H:%M:%S %Z")
                search_time_taken = results['search_metadata']['total_time_taken']
                search_language = results['search_parameters']['hl']
            except Exception as e:
                search_id = None
                search_time = None
                search_time_taken = None
                search_language = None
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print(f"JSON - SerpAPI ERROR!!!: {search_term} in {search_location} JSON file fields have changed!!!")
                print("Following error returned:")
                print(e)

            # convert search results and metadata to a dataframe
            df = pd.DataFrame({'search_term': [search_term],
                                'search_location': [search_location],
                                'result_offset': [result_offset],
                                'error': [error],
                                'search_id': [search_id],
                                'search_time': [search_time],
                                'search_time_taken': [search_time_taken],
                                'search_language': [search_language],
                                'results': [json.dumps(results)]
                                })

            # submit dataframe to BigQuery
            table_id = config.table_id_json
            client = bigquery.Client()
            table = client.get_table(table_id)
            errors = client.insert_rows_from_dataframe(table, df)
            if errors == [[]]:
                print(f"JSON - DATA LOADED: {search_term} in {search_location} loaded into BigQuery {table_id}")
            else:
                print(f"JSON - ERROR!!!: {search_term} in {search_location} NOT loaded into BigQuery {table_id}!!!")
                print("Following error returned from the googles:")
                print(errors)
        except UnboundLocalError as ule:
            # TODO: Need to build something to catch this error sooner
            # GoogleSearch(params) code returns blank results and then get error for  "'df' referenced before assignment" in 'errors = client.insert_rows_from_dataframe(table, df)' (i.e., SerpApi issue)
            print(f"JSON - SerpApi ERROR!!!: Search {result_offset} of {search_term} in {search_location} yielded no results from SerpApi and FAILED load into BigQuery!!!")
            print("Following error returned:")
            print(ule)
            # no sleep requirement as usually an issue with search term provided
        except TimeoutError as te:
            # client.get_table(table_id) code returns TimeOut Exception with no results... so also adding sleep (i.e., BigQuery issue)
            print(f"JSON - BigQuery ERROR!!!: {search_term} in {search_location} had TimeOutError and FAILED to load into BigQuery  {table_id}!!!")
            print("Following error returned:")
            print(te)
            # sleep removed for first implementation 12/31/2022
            # print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
            # time.sleep(ERROR_SLEEP_MIN * 60)
        except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(f"JSON - BigQuery ERROR!!!: {search_term} in {search_location} had an error that needs to be investigated!!!")
            print("Following error returned:")
            print(e)
            # sleep removed for testing
            # print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
            # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # time.sleep(ERROR_SLEEP_MIN * 60)

        return

    def _serpapi_bigquery(search_term, search_location, search_time):
        """
        Function to call SerpApi and insert results into BigQuery {gsearch_jobs_all} used by us_job_postings 
        and non_us_job_postings tasks

        Args:
            search_terms : list
                List of search terms to search for
            search_locations : list
                List of search locations to search for
            search_time : str
                Time period to search for (e.g. 'past 24 hours')

        Returns:
            num_searches : int
                Number of searches performed for this search term and location
        
        Source:
            https://serpapi.com/google-jobs-results
            https://cloud.google.com/bigquery/docs/reference/libraries
        """
        
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
                            # Send JSON request to BigQuery json table
                            _bigquery_json(results, search_term, search_location, num, error)
                            break
                    except KeyError:
                        print(f"SUCCESS SerpApi CALL: {search_term} in {search_location} on search {num}")
                        # Send JSON request to BigQuery json table
                        _bigquery_json(results, search_term, search_location, num, error)
                    else:
                        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                        print(f"SerpApi Error on call!!!: No response on {search_term} in {search_location} on search {num}")
                        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                        error = True
                        break
                except Exception as e: # catching as 'TimeoutError' didn't work so resorted to catching all...
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print(f"SerpApi ERROR (Timeout)!!!: {search_term} in {search_location} had an error (most likely TimeOut)!!!")
                    print("Following error returned:")
                    print(e)
                    print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    time.sleep(ERROR_SLEEP_MIN * 60)
                    error = True
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
                    # 28Dec2022: Following added after SerpApi changed format of json file
                    # wanted to keep extra columns ['job_highlights' 'related_links'] added in json but reached bigquery resource limit
                    # tried to convert these columns to json but ran into error troubleshooting all day
                    # jobs_all['json'] = jobs_all.apply(lambda x: x.to_json(), axis=1)
                    final_columns = ['title',
                    'company_name',
                    'location',
                    'via',
                    'description',
                    'extensions',
                    'job_id',
                    'thumbnail',
                    'posted_at',
                    'schedule_type',
                    'salary',
                    'work_from_home',
                    'date_time',
                    'search_term',
                    'search_location',
                    'commute_time']
                    # select only columns from final_columns if they exist in jobs_all
                    jobs_all = jobs_all.loc[:, jobs_all.columns.isin(final_columns)]

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
                    print(f"SerpApi ERROR!!!: Search {num} of {search_term} in {search_location} yielded no results from SerpApi and FAILED load into non-JSON BigQuery!!!")
                    print("Following error returned:")
                    print(ule)
                    # no sleep requirement as usually an issue with search term provided
                except TimeoutError as te:
                    # client.get_table(table_id) code returns TimeOut Exception with no results... so also adding sleep (i.e., BigQuery issue)
                    print(f"BigQuery ERROR!!!: {search_term} in {search_location} had TimeOutError and FAILED to load into non-JSON BigQuery!!!")
                    print("Following error returned:")
                    print(te)
                    print(f"Sleeping for {ERROR_SLEEP_MIN} minutes")
                    time.sleep(ERROR_SLEEP_MIN * 60)
                except Exception as e:
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print(f"non-JSON BigQuery ERROR!!!: {search_term} in {search_location} had an error that needs to be investigated!!!")
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
        """
        DAG to pull US job postings using the _serpapi_bigquery function

        Args:
            search_terms : list
                List of search terms to search for
            search_locations_us : list
                List of search locations to search for
            context : dict
                Context dictionary from Airflow

        Returns:
            None
        """
        search_time = "date_posted:today"
        total_searches = 0

        for search_term in search_terms:
            for search_location in search_locations_us:
                print(f"START SEARCH: {total_searches} searches done, starting search...")
                num_searches = _serpapi_bigquery(search_term, search_location, search_time)   
                total_searches += num_searches

        # push total_searches to xcom so can use in next task
        context['task_instance'].xcom_push(key='total_searches', value=total_searches)

        return

    us_jobs = PythonOperator(
        task_id='us_jobs',
        provide_context=True,
        op_kwargs={'search_terms': search_terms, 'search_locations_us': search_locations_us},
        python_callable=_us_jobs
    )

    def _non_us_jobs(search_terms, country_percent, **context):
        """
        DAG to pull non-US job postings using the _serpapi_bigquery function

        Args:
            search_terms : list
                List of search terms to search for
            country_percent : pandas dataframe
                Dataframe of countries and their relative percent of total YouTube views for my channel
            context : dict
                Context dictionary from Airflow

        Returns:
            None
        
        Source:
            https://youtube.com/@lukebarousse
        """
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
