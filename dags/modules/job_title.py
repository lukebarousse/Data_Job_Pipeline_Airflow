"""
Transform non-unique job titles into a standard form using BART Zero Shot Text Classification
Eg. 'SR. DATA ENGR' -> 'Senior Data Engineer'

Reference: https://huggingface.co/facebook/bart-large-mnli?candidateLabels=Data+Engineer%2C+Data+Scientist%2C+Data+Analyst%2C+Software+Engineer%2C+Business+Analyst%2C+Machine+Learning+Engineer%2C+Senior+Data+Engineer%2C+Senior+Data+Scientist%2C+Senior+Data+Analyst%2C+Cloud+Engineer&multiClass=false&text=Software+%2F+Data+Engineer
"""
import pandas as pd
from google.cloud import bigquery

# for large datasets store data in google storage for improved speed using the following command
# %bigquery --project job-listings-366015 --use_bqstorage_api 

# for apple silicone m1 macs, use the following to enable MPS
# import torch
# mps_device = torch.device("mps")
# import os
# os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

from transformers import pipeline

def transform_job_title():
    # Initialize the pipeline
    classifier = pipeline("zero-shot-classification",
                        model="facebook/bart-large-mnli",
                        # device=mps_device   # couldn't get this to work
                        )

    # Query to get all unique job titles
    client = bigquery.Client()
    query_job = client.query(
        """
        WITH jobs_all AS (
            SELECT job_title, COUNT(job_title) AS job_title_count
            FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact
            GROUP BY job_title
            ORDER BY job_title_count DESC
        ), jobs_clean AS (
            SELECT job_title, job_title_clean
            FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_job_title
        ), jobs_unclean AS (
            SELECT job_title, job_title_clean, job_title_count
            FROM jobs_all       
            LEFT JOIN jobs_clean
            USING (job_title)
            WHERE job_title_clean IS NULL   
        )

        SELECT *
        FROM jobs_unclean
        ORDER BY job_title_count DESC
        """
    )

    jobs_df = query_job.to_dataframe()
    print("Starting BART pipeline to transform unique job titles: ", len(jobs_df))

    candidate_labels = ['Data Engineer', 'Data Scientist', 'Data Analyst', 'Software Engineer', 'Business Analyst', 'Machine Learning Engineer', 'Senior Data Engineer', 'Senior Data Scientist', 'Senior Data Analyst', 'Cloud Engineer']

    # Iterate over a dataframe
    for index, row in jobs_df.iterrows():
        sequence_to_classify = row['job_title']
        try:
            results = classifier(sequence_to_classify, candidate_labels)
            jobs_df.at[index, 'job_title_clean'] = results['labels'][0]
        except ValueError: #raised when the input is empty
            jobs_df.at[index, 'job_title_clean'] = None

    # Save to csv
    # jobs_df.to_csv('jobs_unclean.csv', index=False)

    # Clean up the data
    jobs_final = jobs_df[~jobs_df.job_title_clean.isnull()]

    jobs_final = jobs_final[['job_title', 'job_title_clean']]

    print("BART complete, uploading to BigQuery")

    # Write to BigQuery
    jobs_final.to_gbq(destination_table='gsearch_job_listings_clean.gsearch_job_title',
                project_id='job-listings-366015',
                if_exists='append')
    
    return